#~ this script will fetch data from the sierra postgresql database and
#~ fill a local database.

import configparser
import sqlite3
import psycopg2
import psycopg2.extras
import os
from datetime import datetime

class App:

	def __init__(self):
		#~ the local database connection
		self.sqlite_conn = None
		#~ the remote database connection
		self.pgsql_conn = None

		#~ open the config file, and parse the options into local vars
		config = configparser.ConfigParser()
		config.read('config.ini')
		self.db_connection_string = config['db']['connection_string']
		self.local_db_connection_string = config['local_db']['connection_string']
		self.itersize = int(config['db']['itersize'])

		#~ open the database connections
		self.open_db_connections()

		#~ create the table if it doesn't exist
		self.create_local_table()

		#~ fill the local database with updated and new bib record data 
		#~ from the transaction table
		self.fill_local_db(deleted=False)
		
		#~ now fill the local database with deleted bib record data from 
		#~ the transaction table
		self.fill_local_db(deleted=True)


	def open_db_connections(self):
		#~ connect to the sierra postgresql server
		try:
			self.pgsql_conn = psycopg2.connect(self.db_connection_string)

		except psycopg2.Error as e:
			print("unable to connect to sierra database: %s" % e)

		#~ connect to the local sqlite database
		try:
			self.sqlite_conn = sqlite3.connect(self.local_db_connection_string)
		except sqlite3.Error as e:
			print("unable to connect to local database: %s" % e)


	def close_connections(self):
		print("closing database connections...")
		if self.pgsql_conn:
			if hasattr(self.pgsql_conn, 'close'):
				print("closing pgsql_conn")
				self.pgsql_conn.close()
				self.pgsql_conn = None

		if self.sqlite_conn:
			if hasattr(self.sqlite_conn, 'close'):
				print("closing sqlite_conn")
				self.sqlite_conn.close()
				self.sqlite_conn = None


	def create_local_table(self):
		cursor = self.sqlite_conn.cursor()
		
		# create the table if it doesn't exist
		sql = """
		CREATE TABLE IF NOT EXISTS `bib_data` (
			`bib_id`	INTEGER UNIQUE,
			`record_num`	INTEGER,
			`record_last_updated`	TEXT,
			`record_last_updated_epoch`	REAL,
			`creation_date`	TEXT,
			`deletion_date`	TEXT,
			`deletion_epoch`	REAL,
			`cataloging_date`	TEXT,
			`best_title`	TEXT,
			`best_author`	TEXT,
			`publish_year`	INTEGER,
			`bib_level_code`	TEXT,
			`material_code`	TEXT,
			`language_code`	TEXT,
			`country_code`	TEXT,
			`control_num_001`	TEXT,
			`control_num_035_is_oclc`	INTEGER,
			`control_num_035`	TEXT,
			PRIMARY KEY(`bib_id`,`record_last_updated_epoch`,`deletion_epoch`)
		);
		"""
		cursor.execute(sql)
		
		sql = """
		CREATE UNIQUE INDEX IF NOT EXISTS `bib_id_index` ON `bib_data` (`bib_id` DESC)
		"""
		cursor.execute(sql)
		
		sql = """
		CREATE INDEX IF NOT EXISTS `deletion_epoch_index` ON `bib_data` (`deletion_epoch` DESC)
		"""
		cursor.execute(sql)
		
		sql = """
		CREATE INDEX IF NOT EXISTS `record_last_updated_epoch_index` ON `bib_data` (`record_last_updated_epoch` DESC)
		"""
		cursor.execute(sql)
		
		self.sqlite_conn.commit()		
		cursor.close()
		cursor = None


	#~ create the table 'data' if it doesn't exist. Grab the largest
	#~ trans_id, and return it.
	def get_local_max(self, deleted=False):
		print('doing deleted?: {}'.format(deleted))
		if deleted == False:
			sql = """
			SELECT
			IFNULL(MAX(record_last_updated_epoch), 0) as max

			FROM
			bib_data

			LIMIT 1
			"""
		elif deleted == True:
			sql = """
			SELECT
			IFNULL(MAX(deletion_epoch), 0) as max

			FROM
			bib_data

			LIMIT 1
			"""

		cursor = self.sqlite_conn.cursor()
		cursor.execute(sql)
		max_id = cursor.fetchone()[0]
		cursor.close()
		cursor = None

		return max_id


	def gen_sierra_bibs(self, start_epoc, deleted=False):
		"""

		here, we'd like to search for bib's where the update time is
		less than the last updated record from our local database

		"""

		sql = """
		SELECT
		r.id,
		r.record_num,
		r.record_last_updated_gmt::date as record_last_update,
		extract(epoch from (r.record_last_updated_gmt)) as record_last_updated_epoch,
		r.creation_date_gmt::date,
		r.deletion_date_gmt::date,
		extract(epoch from (r.deletion_date_gmt)) as deletion_epoch,
		b.cataloging_date_gmt::date,
		p.best_title,
		p.best_author,
		p.publish_year,
		p.bib_level_code,
		p.material_code,
		b.language_code,
		b.country_code,
		(
			SELECT
			v.field_content

			FROM
			sierra_view.varfield as v

			WHERE
			v.record_id = r.id
			AND v.varfield_type_code || v.marc_tag = 'o001'
			-- and v.marc_tag = '001'

			ORDER BY
			v.occ_num

			LIMIT 1

		) as control_num_001,

		(
			SELECT
			CASE
				WHEN v.field_content ~* '\(ocolc\)[0-9]{6,}'
				THEN true
				ELSE false
			END

			FROM
			sierra_view.varfield as v

			WHERE
			v.record_id = r.id
			AND v.varfield_type_code || v.marc_tag = 'o035'
			-- and v.marc_tag = '001'

			ORDER BY
			v.occ_num

			LIMIT 1

		) as control_num_035_is_oclc,

		(
			SELECT
			CASE
				WHEN v.field_content ~* '\(ocolc\)[0-9]{6,}'
				THEN (regexp_matches(v.field_content, '[0-9]{6,}', 'gi'))[1]
				ELSE v.field_content
			END

			FROM
			sierra_view.varfield as v

			WHERE
			v.record_id = r.id
			AND v.varfield_type_code || v.marc_tag = 'o035'
			-- and v.marc_tag = '001'

			ORDER BY
			v.occ_num

			LIMIT 1

		) as control_num_035

		FROM
		sierra_view.record_metadata as r

		LEFT OUTER JOIN
		sierra_view.bib_record_property as p
		ON
		p.bib_record_id = r.id

		LEFT OUTER JOIN
		sierra_view.bib_record as b
		ON
		b.record_id = r.id

		WHERE
		r.record_type_code || r.campus_code = 'b'
		
		"""

		if deleted == False:
			sql += str("AND r.record_last_updated_gmt > to_timestamp(%s)\n")
			sql += str("AND r.deletion_date_gmt IS NULL")
		elif deleted == True:
			sql += str("AND r.deletion_date_gmt IS NOT NULL\n")
			"""
			there isn't very much precision when it comes to deleted 
			records, so the date is going to have to do. We have to be 
			careful to grab dates after our max deletion date, and 
			exactly matching because of the lack of precision
			
			tldr;we're going to have to grab every deleted from the 
			date of the last deleted record because there's no 
			timestamp on the deletion_date_gmt field :(
			
			"""
			sql += str("AND r.deletion_date_gmt::date >= to_timestamp(%s)::date")
			
		#~ debug
		#~ print("start_epoc: {}\nsql :{}".format(start_epoc, sql))

		#~ debug
		#~ sql += "  LIMIT 5000"

		with self.pgsql_conn as conn:
			with conn.cursor(name='latest_bibs_cursor', cursor_factory=psycopg2.extras.DictCursor ) as cursor:
				#~ we want to have the remote database feed us records of self.itersize
				cursor.itersize = self.itersize
				#~ execute the query with the query parameters
				cursor.execute(sql, (start_epoc, ))

				#~ fetch and yield self.itersize number of rows per round
				rows = None
				while True:
					rows = cursor.fetchmany(self.itersize)
					if not rows:
						break

					for row in rows:
						# do something with row
						yield row
		cursor.close()


	def fill_local_db(self, deleted=False):
		"""
		
		Fill the local database by updated / new bibs or update select 
		fields for deleted bib, therefore preserving some metadata for 
		those records

		one possible way to accomplish this: 
		https://stackoverflow.com/questions/2717590/sqlite-insert-on-duplicate-key-update

		"""

		#~ insert or replace into our table, the updated or new bib 
		#~ record data		
		if deleted == False:
			sql = """
			INSERT OR REPLACE INTO
			bib_data (
				'bib_id', --0 INTEGER NOT NULL UNIQUE,
				'record_num', --1 INTEGER,
				'record_last_updated', --2 TEXT
				'record_last_updated_epoch', --3 REAL,
				'creation_date', --4 TEXT,
				'deletion_date', --5 TEXT,
				'deletion_epoch', --6 TEXT,
				'cataloging_date', --7 TEXT,
				'best_title', --8 TEXT,
				'best_author', --9 TEXT,
				'publish_year', --10 INTEGER,
				'bib_level_code', --11 TEXT,
				'material_code', --12 TEXT,
				'language_code', --13 TEXT,
				'country_code', --14 TEXT,
				'control_num_001',  --15 TEXT,
				'control_num_035_is_oclc', --16 INTEGER,
				'control_num_035' --17 TEXT
			)

			VALUES (
				?, --0
				?, --1
				?, --2
				?, --3
				?, --4
				?, --5
				?, --6
				?, --7
				?, --8
				?, --9
				?, --10
				?, --11
				?, --12
				?,  --13
				?,  --14
				?,  --15
				?,  --16
				?  --17
			)
			"""
			
		elif deleted == True:
			sql = """
			INSERT OR REPLACE INTO
			bib_data (
				'bib_id', --0 INTEGER NOT NULL UNIQUE,
				'record_num', --1 INTEGER,
				'record_last_updated', --2 TEXT
				'record_last_updated_epoch', --3 REAL,
				'creation_date', --4 TEXT,
				'deletion_date', --5 REAL,
				'deletion_epoch', --6 TEXT,
				'cataloging_date', --7 TEXT,
				'best_title', --8 TEXT,
				'best_author', --9 TEXT,
				'publish_year', --10 INTEGER,
				'bib_level_code', --11 TEXT,
				'material_code', --12 TEXT,
				'language_code', --13 TEXT,
				'country_code', --14 TEXT,
				'control_num_001',  --15 TEXT,
				'control_num_035_is_oclc', --16 INTEGER,
				'control_num_035' --17 TEXT
			)

			VALUES (
				?, --0	420909816679
				?, --1	3021671
				?, --2	"2016-06-15" 
				?, --3	1466014679.0
				?, --4	"2015-01-19"
				?, --5	null
				?, --6	0.0
				-- preserve the rest if it had existed before
				(SELECT cataloging_date FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --7	"2015-01-19"
				(SELECT best_title FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --8	"Dogwood Hill [electronic resource]"
				(SELECT best_author FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --9	"Woods, Sherryl, author."
				(SELECT publish_year FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --10	2015
				(SELECT bib_level_code FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --11	"m"
				(SELECT material_code FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1), --12	"2"
				(SELECT language_code FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1),  --13 "eng"
				(SELECT country_code FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1),  --14 "onc"
				(SELECT control_num_001 FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1),  --15 "|aovd84EB6AB7-71B7-47C2-8AC5-847A6B209530"
				(SELECT control_num_035_is_oclc FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1),  --16 null
				(SELECT control_num_035 FROM bib_data WHERE bib_data.bib_id = ? LIMIT 1)  --17 null
			)
			
			"""
			
			
		#~ get the local max (sending if deleted or not)
		local_max = self.get_local_max(deleted)
		print('starting with max date: \t{}'.format(local_max))
		cursor = self.sqlite_conn.cursor()

		counter = 0
		
		for row in self.gen_sierra_bibs(local_max, deleted):

			#~ debug
			#~ print(row)

			#~ TODO
			#~ make sure we handle cases where the null value is returned for the column value

			#~ record_last_updated_epoch and deletion_epoch can not be 
			#~ null, so make sure of that here
			if row['record_last_updated_epoch'] is None:
				record_last_updated_epoch = 0 
			else:
				record_last_updated_epoch = float(row['record_last_updated_epoch'])

			if row['deletion_epoch'] is None:
				deletion_epoch = 0 
			else:
				deletion_epoch = float(row['deletion_epoch'])

			
			#~ set the values depending on if we're inserting deleted 
			#~ values or not
			
			if deleted == False:
				values = (
					int(row['id']),
					int(row['record_num']),
					row['record_last_update'],
					record_last_updated_epoch,
					row['creation_date_gmt'],
					row['deletion_date_gmt'],
					deletion_epoch,
					row['cataloging_date_gmt'],
					row['best_title'],
					row['best_author'],
					row['publish_year'],
					row['bib_level_code'],
					row['material_code'],
					row['language_code'],
					row['country_code'],
					row['control_num_001'],
					row['control_num_035_is_oclc'],
					row['control_num_035']
				)
				
			elif deleted == True:
				values = (
					int(row['id']),
					int(row['record_num']),
					row['record_last_update'],
					record_last_updated_epoch,
					row['creation_date_gmt'],
					row['deletion_date_gmt'],
					deletion_epoch,
					row['cataloging_date_gmt'],
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id']),
					int(row['id'])
				)

			#~ debug
			#~ print(values)
			
			cursor.execute(sql, values)
			
			#~ we are going to insert if it doesn't exist, and if we 
			#~ reaise an integrity error (duplicate key), update in the except
			#~ try:
				#~ cursor.execute(sql, values)
				
			#~ except sqlite3.IntegrityError as error:
				#~ print("last row id: ", end='')
				#~ print(cursor.lastrowid)
				#~ print(error)
				
				#~ pass
				
								
			#~ TODO: perform a second update query on records that have been deleted		
			
			
			#~ probably should commit every self.itersize rows
			counter += 1
			if(counter % self.itersize == 0):
				self.sqlite_conn.commit()
				print('counter: {}'.format(counter))
				print('id: {}'.format(row['id']))
				print(values)
		
		self.sqlite_conn.commit()
		#~ fixes the error "UnboundLocalError: local variable 'row' 
		#~ referenced before assignment" where there are no rows returned 
		#~ from query		
		if 'row[0]' in locals():
			print('finishing with id: \t{}'.format(row['id']))
		print('final count inserted ("deleted"?:{}): \t\t{}'.format(deleted, counter))
		cursor.close()
		cursor = None


	#~ the destructor
	def __del__(self):
		self.sqlite_conn.commit()
		self.close_connections()
		print("done.")


#~ run the app!
start_time = datetime.now()
print('starting import at: \t\t{}'.format(start_time))
app = App()
end_time = datetime.now()
print('finished import at: \t\t{}'.format(end_time))
print('total import time: \t\t{}'.format(end_time - start_time))

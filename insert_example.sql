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
	420909816679, --0	420909816679
	3021671, --1	3021671
	"2018-05-23", --2	"2016-06-15" 
	1527080425.351, --3	1466014679.0
	"2015-01-19", --4	"2015-01-19"
	"2018-05-23", --5	null
	1527033600, --6	0.0
	-- preserve the rest if it had existed before
	(SELECT cataloging_date FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --7	"2015-01-19"
	(SELECT best_title FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --8	"Dogwood Hill [electronic resource]"
	(SELECT best_author FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --9	"Woods, Sherryl, author."
	(SELECT publish_year FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --10	2015
	(SELECT bib_level_code FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --11	"m"
	(SELECT material_code FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1), --12	"2"
	(SELECT language_code FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1),  --13 "eng"
	(SELECT country_code FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1),  --14 "onc"
	(SELECT control_num_001 FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1),  --15 "|aovd84EB6AB7-71B7-47C2-8AC5-847A6B209530"
	(SELECT control_num_035_is_oclc FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1),  --16 null
	(SELECT control_num_035 FROM bib_data WHERE bib_data.bib_id = 420909816679 LIMIT 1)  --17 null
)
																	

-- bib_data.bib_id = 420909816679

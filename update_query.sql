-- select 
-- to_timestamp(1447696669.493)


SELECT
r.id,
r.record_num,
extract(epoch from (r.record_last_updated_gmt)) as record_last_updated_epoch,
r.record_last_updated_gmt,
r.creation_date_gmt::date,
r.deletion_date_gmt::date,
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
	
) as control_num

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
                                              
AND r.record_last_updated_gmt > to_timestamp(1526134272.0)
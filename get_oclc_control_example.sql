SELECT

(
	SELECT
	-- get the first series of numbers from the marc 035 (where the varfield type code is 'o')
	substring(v.field_content, '[0-9]{6,}') as oclc_control

	FROM
	sierra_view.varfield as v

	WHERE
	v.record_id = r.id
	AND v.varfield_type_code || v.marc_tag = 'o035'

	LIMIT 1
) AS oclc_control,
*


from
sierra_view.record_metadata as r

where
-- r.record_num = 3266874
r.record_type_code = 'b'

limit
100
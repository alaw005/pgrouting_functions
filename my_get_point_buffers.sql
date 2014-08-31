/*
Name:
	my_get_point_buffers - Generates a buffer around specified geometry in table and returns buffer results
	
Description:
	my_buffer(source_sql text, radius_of_buffer float, srid integer);
	source_sql: Must be in format and have fields "SELECT id::integer, the_geom::geography FROM tablename"
	srid: function will return geometry with specified srid

Usage:
	SELECT * FROM hutt_pax.my_buffer('SELECT stop_id::integer AS id, the_geom FROM gtfs_stops', 400, 2193);

Author:
	Author Adam Lawrence <alaw005@gmail.com>
*/
DROP FUNCTION IF EXISTS my_get_point_buffers(text, float, integer);
CREATE OR REPLACE FUNCTION my_get_point_buffers(
	source_sql text,	-- 'SELECT id::integer, the_geom::geometry FROM ...'
	radius_of_buffer float DEFAULT 400,
	srid integer DEFAULT 2193
)
	RETURNS TABLE(
		id integer, 
		source_id integer,
		the_geom geometry
	) AS
$BODY$
DECLARE
	
BEGIN

	-- Create temp table with incremental id field 
	CREATE TEMP TABLE my_buffer_tmp_table (
		id serial PRIMARY KEY,
		source_id integer,
		the_geom geometry
	) ON COMMIT DROP;

	-- Generate buffer around points
	EXECUTE 'INSERT INTO my_buffer_tmp_table (source_id, the_geom) 
		SELECT a.id AS source_id, ST_Buffer(ST_Transform(a.the_geom, ' || srid || '), ' || radius_of_buffer || ') AS the_geom
		FROM (' || source_sql || ') AS a';
	
	-- Noted
	RETURN QUERY SELECT * FROM my_buffer_tmp_table;

END;
$BODY$ 
LANGUAGE plpgsql VOLATILE;
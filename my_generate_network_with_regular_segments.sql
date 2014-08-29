/*
Name:
	my_generate_network_with_regular_segments - generates network with regular segments (e.g. every 20m) from existing network 

Notes:
	1. Due to pgrouting bug outputTableName must be specified for a schema owned by
       user otherwise pgr_createTopology does not generate source and target values
	2. Only suitable for smaller networks, as otherwise gets too cumbersome and runs for days!

Usage:

  SELECT ST_AsText(the_geom) 
    FROM my_generate_network_with_regular_segments('schema.roadNetwork', 'schema.segmentedNetwork', 'the_geom', 20.00);

This function was inspired by an example from internet which had following comments:
--The below example simulates a while loop in
--SQL using PostgreSQL generate_series() to cut all
--linestrings in a table to 100 unit segments
-- of which no segment is longer than 100 units
-- units are measured in the SRID units of measurement
-- It also assumes all geometries are LINESTRING or contiguous MULTILINESTRING
--and no geometry is longer than 100 units*10000
--for better performance you can reduce the 10000
--to match max number of segments you expect

Author:
	Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS my_generate_network_with_regular_segments(text, text, text, double precision, integer);
CREATE OR REPLACE FUNCTION my_generate_network_with_regular_segments(
    inputtablename text, 
	outputtablename text, 
	geomfieldname text DEFAULT 'the_geom'::text, 
	spacing double precision DEFAULT 100.00, 
	GeometrySRID integer DEFAULT 2193
)
	RETURNS TABLE(
		id integer, 
		source bigint, 
		target bigint, 
		distance double precision, 
		the_geom geometry) AS
$BODY$
DECLARE
	-- Nothing at this stage
BEGIN

	-- Create temp table as source (doing this so can transform to specified GeometrySRID
	CREATE TEMP TABLE tmp_source (
		id serial PRIMARY KEY,
		the_geom geometry,
		length double precision
	) ON COMMIT DROP;

	-- Populate temp table with data from inputTableName, 
	-- Note that transforming to the specified geometrySRID
	EXECUTE 'INSERT INTO tmp_source (the_geom, length) 
		SELECT ST_LineMerge(ST_Transform(a.' || geomFieldName || ',' || GeometrySRID || ')) AS the_geom,
		ST_Length(ST_Transform(a.' || geomFieldName ||',' || GeometrySRID || ')) As length
		FROM ' || inputTableName || ' AS a 
		WHERE ST_Length(a.' || geomFieldName ||') <> 0';

	-- Create table for the new network (note cannot use temporary table due to pgr_createTopology bug)
	-- DROP on commit so we do not get problems reusing it again in this session
	-- Note specifyihng geometry projection in table creation
	EXECUTE 'DROP TABLE IF EXISTS ' || outputTableName;
	EXECUTE 'DROP TABLE IF EXISTS ' || outputTableName || '_vertices_pgr';
	EXECUTE 'CREATE TABLE ' || outputTableName || '  (
	    id serial PRIMARY KEY,
            source bigint,
            target bigint,
            distance double precision,
            the_geom geometry
	)';

	-- Main section, generate new table data, from temporary table above
	-- Note using ST_SetSRID to set geometrySRID to that specified (otherwise doesn't come up in QGIS)
	EXECUTE 'INSERT INTO ' || outputTableName || ' (the_geom) 
		SELECT ST_SetSRID(ST_AsText(ST_LineSubstring(t.the_geom, ' || spacing || '*n/length,
		  CASE
			WHEN ' || spacing || '*(n+1) < length THEN ' || spacing || '*(n+1)/length
			ELSE 1
		  END)),' || GeometrySRID || ') As the_geom
		FROM tmp_source AS t
		CROSS JOIN generate_series(0,' || spacing*100 || ') AS n
		WHERE n*' || spacing || '/length < 1';

	-- Create network topology
	-- NOte due to bug outputTableName must not be in public schema (https://github.com/pgRouting/osm2pgrouting/issues/28) 
	EXECUTE 'SELECT pgr_createTopology(''' || outputTableName || ''',' || 0.001 || ')';

	-- Create distance
	EXECUTE 'UPDATE ' || outputTableName || ' AS a SET distance = ST_Length(a.the_geom)';

	--Return table results to calling function (note table created also)
	RETURN QUERY EXECUTE 'SELECT * FROM ' || outputTableName;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

/*
Description:
  Function will return catchment distance (in metres) and catchment polygon area 
  calculated along the network specified for rows in table provided
	
Example usage:
  SELECT * 
  INTO hutt_pax.tmp_stop_catchment
  FROM hutt_pax.my_pgr_createCatchmentArea(
  	'SELECT 
	      stop_id::integer AS id, 
	      the_geom::geometry AS the_geom
	FROM gtfs_stops 
	WHERE stop_id IN(''10446'',''10415'',''10427'')',
  	'hutt_pax.wainuiomata_roads_20m'
  );

Notes:
  Currently values are hard coded in declare section
  
*/
--DROP FUNCTION IF EXISTS hutt_pax.my_pgr_createCatchmentArea(text, text, integer, integer);
CREATE OR REPLACE FUNCTION hutt_pax.my_pgr_createCatchmentArea(
	sourceSql text,  -- e.g. 'SELECT id::integer, the_geom::geometry FROM pointTable)'
	roadNetworkTableName text,   -- Note needs to have associated vertices table ending with "_vertices_pgr"
	catchmentDistance integer DEFAULT 400,
	geometrySRID integer DEFAULT 2193
)
	RETURNS TABLE(
		id integer, 
		source_id bigint,
		node_id bigint, 
		catchment integer,
		geom geometry
	) AS
$BODY$
DECLARE

	r RECORD;
	node_id integer;
	poly_geom geometry;
	
BEGIN

	-- Temp table to hold closest node_id 
	EXECUTE 'CREATE TEMP TABLE my_pgr_createCatchmentArea_sourceNodes (
		id serial PRIMARY KEY, 
		source_id bigint,
		node_id bigint
	) ON COMMIT DROP';

	CREATE TEMP TABLE my_pgr_createCatchmentArea_table (
		id serial PRIMARY KEY, 
		source_id bigint,
		node_id bigint,
		catchment integer,
		geom geometry
	) ON COMMIT DROP;

	-- Loop through source point records
	FOR r IN EXECUTE sourceSql
	LOOP

		-- Get nearest node_id from network layer for current source_id record
		EXECUTE 'INSERT INTO my_pgr_createCatchmentArea_sourceNodes (source_id, node_id) 
				SELECT a.id, b.id AS node_id
				FROM (' || sourceSql || ') AS a, ' || roadNetworkTableName || '_vertices_pgr AS b 
			WHERE a.id::integer =  ' || r.id || '
			ORDER BY ST_distance(ST_Transform(a.the_geom, ' || geometrySRID || '), ST_Transform(b.the_geom, ' || geometrySRID || ')) 
			LIMIT 1';

		-- Assign node_id to function variable (note had to do this separately because dynamic sources)
		SELECT INTO node_id a.node_id FROM my_pgr_createCatchmentArea_sourceNodes AS a WHERE a.source_id = r.id LIMIT 1;

		-- Apply driving distance to determine catchment area for current source 
		-- and generate catchment area polygon 
		SELECT INTO poly_geom
			ST_SetSRID(ST_Buffer(pgr_pointsaspolygon,50),geometrySRID) AS geom
		FROM pgr_pointsAsPolygon(
				'SELECT 
					b.id::int4 AS id, 
					ST_X(ST_Transform(b.the_geom, ' || geometrySRID || '))::float8 AS x, 
					ST_Y(ST_Transform(b.the_geom, ' || geometrySRID || '))::float8 AS y 
				FROM pgr_drivingdistance(
					''''SELECT 
						id::int4,
						source::int4,
						target::int4,
						distance::float8 AS cost
					FROM ' || roadNetworkTableName || ''''',
					' || node_id || ',
					' || catchmentDistance || ',
					false,
					false) AS a
				JOIN ' || roadNetworkTableName || '_vertices_pgr AS b ON a.id1 = b.id');


		-- Insert record into temp table
		INSERT INTO my_pgr_createCatchmentArea_table (source_id, node_id, catchment, geom) VALUES (r.id, node_id, catchmentDistance, poly_geom);

	END LOOP;

	-- Return results from templ table
	RETURN QUERY SELECT * FROM my_pgr_createCatchmentArea_table;
END;
$BODY$ 
LANGUAGE plpgsql VOLATILE;

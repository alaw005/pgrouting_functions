/*
Name:
	my_get_network_catchment - Generates a network catchment using specified network 
	
Description:
	Function will return catchment distance (in metres) and catchment polygon area 
	calculated along the network specified for rows in table provided
	
Usage:
	SELECT * 
	INTO hutt_pax.tmp_stop_catchment
	FROM my_get_network_catchment(
	'SELECT 
		  stop_id::integer AS id, 
		  the_geom::geometry AS the_geom
	FROM gtfs_stops 
	WHERE stop_id IN(''10446'',''10415'',''10427'')',
	'hutt_pax.wainuiomata_roads_20m',
	400,
	2193,
	'distance'
	);

Author:
	Adam Lawrence <alaw005@gmail.com>  
*/
DROP FUNCTION IF EXISTS my_get_network_catchment(text, text, float, integer, text);
CREATE OR REPLACE FUNCTION my_get_network_catchment(
	sourceSql text,  -- e.g. 'SELECT id::integer, the_geom::geometry FROM pointTable)'
	roadNetworkTableName text,   -- Note needs to have associated vertices table (generated using pgr_createTopology) and ending with "_vertices_pgr"
	catchmentDistance float,
	geometrySRID integer DEFAULT 2193,
	costfield text DEFAULT 'distance'
)
	RETURNS TABLE(
		id integer, 
		source_id bigint,
		node_id bigint, 
		catchment float,
		geom geometry
	) AS
$BODY$
DECLARE

	r RECORD;
	node_id integer;
	poly_geom geometry;
	
BEGIN

	-- Temp table to hold closest node_id 
	CREATE TEMP TABLE tmp_source_nodes_table (
		id serial PRIMARY KEY, 
		source_id bigint,
		node_id bigint
	) ON COMMIT DROP;

	-- Temp table to hold generated catchment
	CREATE TEMP TABLE tmp_catchment_table (
		id serial PRIMARY KEY, 
		source_id bigint,
		node_id bigint,
		catchment float,
		geom geometry
	) ON COMMIT DROP;

	-- Loop through source point records
	FOR r IN EXECUTE sourceSql
	LOOP

		-- Get nearest node_id from network layer for current source_id record
		EXECUTE 'INSERT INTO tmp_source_nodes_table (source_id, node_id) 
				SELECT a.id, b.id AS node_id
				FROM (' || sourceSql || ') AS a, ' || roadNetworkTableName || '_vertices_pgr AS b 
			WHERE a.id::integer =  ' || r.id || '
			ORDER BY ST_distance(ST_Transform(a.the_geom, ' || geometrySRID || '), ST_Transform(b.the_geom, ' || geometrySRID || ')) 
			LIMIT 1';

		-- Assign node_id to function variable (note had to do this separately because dynamic sources)
		SELECT INTO node_id a.node_id FROM tmp_source_nodes_table AS a WHERE a.source_id = r.id LIMIT 1;

		-- Apply driving distance to determine catchment area for current source 
		-- and generate catchment area polygon. Have added error check as pgr_pointsAsPolygon generates internal error
		-- if less than three data points are returned
		BEGIN
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
							' || costfield || '::float8 AS cost
						FROM ' || roadNetworkTableName || ''''',
						' || node_id || ',
						' || catchmentDistance || ',
						false,
						false) AS a
					JOIN ' || roadNetworkTableName || '_vertices_pgr AS b ON a.id1 = b.id');


			-- Insert record into temp table
			INSERT INTO tmp_catchment_table (source_id, node_id, catchment, geom) VALUES (r.id, node_id, catchmentDistance, poly_geom);

		EXCEPTION 
			WHEN internal_error THEN -- Do nothing, this error arises if not enough points
		END;

	END LOOP;

	-- Return results from temp table
	RETURN QUERY SELECT * FROM tmp_catchment_table;
END;
$BODY$ 
LANGUAGE plpgsql VOLATILE;


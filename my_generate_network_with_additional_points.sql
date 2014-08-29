/*
Name:
	my_generate_network_with_additional_points - Generates network with additional network nodes from specified sql
	
Description:
	sourcePointSql: Additional points to add to network. The sql must be in form "SELECT id::integer, the_geom::geometry FROM table"
	networkEdgesSql: Current network geometries. The sql must be in form "SELECT id::integer, the_geom::geometry, km::float8, kmh::integer FROM table"
	outputTableName: The new network table that will be created

Usage:
	SELECT my_generate_network_with_additional_points(
		'SELECT stop_id::integer AS id, the_geom AS the_geom FROM gtfs_stops WHERE stop_id IN( ''10415'',''10417'',''10424'')',
		'SELECT id_0::integer AS id, geom AS the_geom, km, kmh FROM roads_nz_wellington',
		'hutt_pax.tmp_test'
	);
	SELECT my_generate_network_with_additional_points(
		'SELECT stop_id::integer AS id, the_geom AS the_geom FROM gtfs_stops',
		'SELECT id_0::integer AS id, geom AS the_geom, km, kmh FROM roads_nz_wellington',
		'hutt_pax.roads_stops_as_nodes'
	);

Author:
	Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS my_generate_network_with_additional_points(text, text, text);
CREATE OR REPLACE FUNCTION my_generate_network_with_additional_points(
	sourcePointSql text DEFAULT 'SELECT id::integer, the_geom:geometry FROM pointstable', 
	networkEdgesSql text DEFAULT 'SELECT id:integer, the_geom:geometry, km::float, kmh::integer FROM networktable', 
	outputTableName text DEFAULT 'my_generated_network'
)
	RETURNS text AS
$BODY$
DECLARE

BEGIN

	-- Create temp table to hold the new nodes snapped to the network
	RAISE NOTICE 'Creating tempory table to hold new nodes';
	CREATE TEMP TABLE tmp_new_nodes (
		id serial PRIMARY KEY,
		new_node_id bigint,
		old_edge_id bigint,
		the_geom geometry
	) ON COMMIT DROP;
	CREATE INDEX idx_tmp_new_nodes_the_geom ON tmp_new_nodes USING gist(the_geom); 
	
	-- Locate source points to the nearest network edge and insert into temp table
	RAISE NOTICE 'Inserting new nodes into temporary table (this may take some time .. around 100 ms per node)';
	EXECUTE 'INSERT INTO tmp_new_nodes (new_node_id, old_edge_id, the_geom)
		SELECT 
			DISTINCT ON (a.id) 
			a.id AS new_node_id,
			b.id AS old_edge_id,
			ST_SetSRID(ST_ClosestPoint(ST_Transform(b.the_geom, 2193), ST_Transform(a.the_geom, 2193)),2193) AS the_geom
		FROM (' || sourcePointSql || ') AS a 
			JOIN (' || networkEdgesSql || ') AS b ON ST_DWithin(ST_Transform(a.the_geom, 2193), ST_Transform(b.the_geom, 2193), 50)
		ORDER BY  
			a.id, 
			ST_distance(ST_Transform(a.the_geom, 2193), ST_Transform(b.the_geom, 2193))';
		

	-- Create table for the new network layer with additional nodes (note cannot use temporary table due to pgr_createTopology bug)
	-- Dropping table ..._vertices_pgr which created when generate topology
	RAISE NOTICE 'Create table for the new network layer with additional nodes';
	EXECUTE 'DROP TABLE IF EXISTS ' || outputTableName;
	EXECUTE 'DROP TABLE IF EXISTS ' || outputTableName || '_vertices_pgr';
	EXECUTE 'CREATE TABLE ' || outputTableName || '  (
	    id serial PRIMARY KEY,
		new_node_id bigint,
		old_edge_id bigint,
        source bigint,
        target bigint,
        km double precision,
		kmh double precision,
        the_geom geometry
	)';
	EXECUTE 'CREATE INDEX idx_' || translate(outputTableName,'.','_') || '_the_geom ON ' || outputTableName || ' USING gist(the_geom)'; 

	-- Generate new network layer
	RAISE NOTICE 'Generating new network layer with additional nodes';
	EXECUTE 'INSERT INTO ' || outputTableName || ' (new_node_id, old_edge_id, km, kmh, the_geom) 
		SELECT 
			CASE 	-- This is to make query easier later as will be able to link directly to node without spatial join
				WHEN n=2 THEN new_nodes.new_node_id
				ELSE NULL
			END AS new_node_id,
			network.id AS old_edge_id,
			CASE 	-- Return new distances for each part of split line
				WHEN n=1 THEN ST_Length(ST_LineSubstring(network.the_geom,0,ST_LineLocatePoint(network.the_geom, new_nodes.the_geom)))/1000
				WHEN n=2 THEN ST_Length(ST_LineSubstring(network.the_geom,ST_LineLocatePoint(network.the_geom, new_nodes.the_geom),1))/1000
				ELSE network.km
			END AS km,	
			network.kmh,
			CASE 	-- Split the line based on location of new node. This works because of generated series  returning two records where split 
				WHEN n=1 THEN ST_LineSubstring(network.the_geom,0,ST_LineLocatePoint(network.the_geom, new_nodes.the_geom))
				WHEN n=2 THEN ST_LineSubstring(network.the_geom,ST_LineLocatePoint(network.the_geom, new_nodes.the_geom),1)
				ELSE network.the_geom
			END AS the_geom 
		FROM (SELECT new_node_id, old_edge_id, the_geom, n FROM tmp_new_nodes CROSS JOIN generate_series(1,2) AS n) AS new_nodes
			RIGHT JOIN (' || networkEdgesSql || ') AS network ON network.id = new_nodes.old_edge_id';

	-- Generate typology for new network
	-- Note due to bug outputTableName must be in user owned schema (e.g not be in public schema) (https://github.com/pgRouting/osm2pgrouting/issues/28) 
	RAISE NOTICE 'Creating typology for new network layer';
	EXECUTE 'SELECT pgr_createTopology(''' || outputTableName || ''',' || 0.001 || ')';

	--Delete null sources
	RAISE NOTICE 'Delete null source and destination records';
	EXECUTE 'DELETE FROM ' || outputTableName || ' WHERE source is null OR destination is null';
	
	RETURN 'OK';  

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
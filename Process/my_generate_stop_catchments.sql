/*
Process:
	Create 400m bus stop catchments along road network

Notes:
		*** This is process that I ran and it worked. Note that where refer separate process these steps
		were undertaken prior, but had tried to document here as much as possible
		
Dependencies:
	Data
		- gtfs_stops
		- roads_nz
		- addresses_nz
	Custom functions
		- my_generate_network_with_additional_points(text, text, text)
		- my_get_point_buffers(text, double precision, integer)
		- my_get_network_catchment(text, text, double precision, integer, text)
		
Author:
	Adam Lawrence <alaw005@gmail.com>
*/
-------------------------------------------------
-- 1A. Import bus stop points from GTFS  (create dataset "gtfs_stops")
-------------------------------------------------
-- Refer separate process to import gtfs dataset to postgis

-------------------------------------------------
-- 1B. Import address points from external source (create dataset "addresses_nz")
-- NB: This is used below to generate additional network nodes for catchment calculation
-------------------------------------------------
-- TODO: Refer separate process to import addresses_nz dataset to postgis

-------------------------------------------------
-- 1C. Import road (walk) network from OSM dataset (create dataset "roads_nz_wellington")
-------------------------------------------------
-- TODO: Refer separate process to import roads_nz dataset to postgis and  extract Wellington only roads.

-------------------------------------------------
-- 2. Add bus stop nodes to road network (create dataset "roads_nz_wellington_stops_as_nodes")
-------------------------------------------------
SELECT my_generate_network_with_additional_points(
	'SELECT stop_id::integer AS id, the_geom FROM gtfs_stops',	--  WHERE stop_id IN (''10445'',''10448'')
	'SELECT id, geom AS the_geom, km, kmh FROM roads_nz_wellington',
	'hutt_pax.roads_nz_wellington_stops_as_nodes'
);

-------------------------------------------------
-- 3A. Create 400m buffer around bus stops (create dataset "tmp_gtfs_stops_400m_buffer")
-- NB: Buffer is used to select subset of addresses in next step
-------------------------------------------------
DROP TABLE IF EXISTS hutt_pax.tmp_gtfs_stops_400m_buffer;
SELECT * INTO hutt_pax.tmp_gtfs_stops_400m_buffer
FROM my_get_point_buffers('SELECT stop_id::integer AS id, the_geom FROM gtfs_stops', 400, 2193);	--  WHERE stop_id IN (''10445'',''10448'')

-------------------------------------------------
-- 3B. Import address points within 400m buffer into temporary dataset (create dataset "tmp_addresses_wellington_400m_crowflies")
-- NB: This dataset is then used to create additional road network nodes (one for each address). We limit
--     to addresses in catchment to reduce overall size of network (which will be huge anyway!)
-------------------------------------------------
DROP TABLE IF EXISTS hutt_pax.tmp_addresses_wellington_400m_crowflies;
SELECT DISTINCT addresses_nz.*
INTO hutt_pax.tmp_addresses_wellington_400m_crowflies 
FROM addresses_nz, hutt_pax.tmp_gtfs_stops_400m_buffer 
WHERE ST_Contains(hutt_pax.tmp_gtfs_stops_400m_buffer.the_geom, addresses_nz.geom);

-------------------------------------------------
-- 3C. Add nodes to road network for each address (create dataset "roads_nz_wellington_stops_and_addresses_as_nodes")
-- NB: Expect to take around 17,000,000 ms (about 4 hours) 
-------------------------------------------------
SELECT my_generate_network_with_additional_points(
	'SELECT id_0::integer AS id, geom AS the_geom FROM hutt_pax.tmp_addresses_wellington_400m_crowflies',
	'SELECT id, the_geom, km, kmh FROM hutt_pax.roads_nz_wellington_stops_as_nodes',
	'hutt_pax.roads_nz_wellington_stops_and_addresses_as_nodes'
);

-------------------------------------------------
-- 4. Create bus stop network catchments (create dataset "gtfs_stops_network_catchments")
-- NB: Expect to take around  7,000,000 ms
-------------------------------------------------
DROP TABLE IF EXISTS hutt_pax.gtfs_stops_network_catchments;
SELECT * 
	INTO hutt_pax.gtfs_stops_network_catchments
	FROM my_get_network_catchment(
	'SELECT 
		  stop_id::integer AS id, 
		  the_geom::geometry AS the_geom
	FROM gtfs_stops',	--  WHERE stop_id IN (''10445'',''10448'')
	'hutt_pax.roads_nz_wellington_stops_and_addresses_as_nodes',
	0.4,
	2193,
	'km'
	);
	
-------------------------------------------------
-- DONE!
-------------------------------------------------

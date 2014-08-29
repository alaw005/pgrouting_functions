/*
	Process followed to generate buffers around all gtfs_stops in Wellington. 
	Uses a couple of custom  functions to generate 1km buffer around bus stops and 
	return the road network within this before which is then divided into 20m segments
	for the 400m walk catchment to be generated.
	
	NB: This is very slow at present, there are sure to be some efficiencies in how works
	
	Adam Lawrence <alaw005@gmail.com>

*/
DROP FUNCTION IF EXISTS my_pgr_calculate_catchment(id integer, the_geom geometry);
CREATE OR REPLACE FUNCTION my_pgr_calculate_catchment()
  RETURNS text AS
$BODY$
DECLARE
	-- Nothing to declare
BEGIN

	DROP TABLE IF EXISTS hutt_pax.tmp_roads_in_buffer;
	SELECT a.*
	INTO hutt_pax.tmp_roads_in_buffer
	FROM public.roads_nz_wellington AS a
		INNER JOIN (
			SELECT * 
			FROM hutt_pax.my_buffer('SELECT stop_id::integer AS id, the_geom FROM gtfs_stops WHERE stop_id=''10418''', 1000.0, 2193)
			) AS b
		ON ST_Intersects(a.geom, b.the_geom);
  
	-- Select all roads within a buffer generated using my buffer function
	-- Query returned successfully: 1104642 rows affected, 22353 ms execution time
	DROP TABLE IF EXISTS hutt_pax.tmp_roads_in_buffer;
	SELECT a.*
	INTO hutt_pax.tmp_roads_in_buffer
	FROM public.roads_nz_wellington AS a
		INNER JOIN (
			SELECT * 
			FROM hutt_pax.my_buffer('SELECT stop_id::integer AS id, the_geom FROM gtfs_stops', 1000.0, 2193)
			) AS b
		ON ST_Intersects(a.geom, b.the_geom);


	-- Create spatial index, hopefully will improve performance for next query
	-- Query returned successfully with no result in 29188 ms.
	DROP INDEX IF EXISTS hutt_pax.sidx_tmp_roads_in_buffer_geom;
	CREATE INDEX sidx_tmp_roads_in_buffer_geom ON hutt_pax.tmp_roads_in_buffer USING gist(geom);

	  
	-- Generate segmented nework from above subset (note this generates a new table as specified in second argument
	-- Query ran for 1.5 million ms before getting to create topology, 
	-- now at  1.9 million ms and creating topology, has completed performing checks 
	-- now at  3.3 million ms and  232000 edges processed, has completed creating topology
	-- now at  3.5 million ms and  266000 edges processed
	-- now at  7.7 million ms and 1570000 edges processed
	-- now at  9.4 million ms and 2110000 edges processed
	-- now at 16.3 million ms and 4380000 edges processed
	--PL/pgSQL function hutt_pax.my_pgr_createsegmentednetwork(text,text,text,double precision,integer) line 48 at EXECUTE statement
	--Total query runtime: 24327627 ms.
	--5970305 rows retrieved.
	SELECT ST_AsText(the_geom) 
	   FROM hutt_pax.my_pgr_createSegmentedNetwork('hutt_pax.tmp_roads_in_buffer', 'hutt_pax.tmp_roads_in_buffer_20m', 'geom', 20.00);

   
	-- Generate catchment areas using segmented road
	-- Query results ... crashed when I did something at 43 million ms (overnight)
	SELECT * 
	  INTO hutt_pax.stop_catchment_20m_wellington
	  FROM hutt_pax.my_pgr_createCatchmentArea(
		'SELECT 
			  stop_id::integer AS id, 
			  the_geom::geometry AS the_geom
		FROM gtfs_stops',
		'hutt_pax.tmp_roads_in_buffer_20m'
	  );

	RETURN 'OK';  

END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
  
  
 



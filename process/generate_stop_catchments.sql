/*
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
*/

/*
-- Create spatial index, hopefully will improve performance for next query
-- Query returned successfully with no result in 29188 ms.
DROP INDEX IF EXISTS hutt_pax.sidx_tmp_roads_in_buffer_geom;
CREATE INDEX sidx_tmp_roads_in_buffer_geom ON hutt_pax.tmp_roads_in_buffer USING gist (geom);
*/
  
-- Generate segmented nework from above subset (note this generates a new table as specified in second argument
-- Query is running 570000 ms so far ... not at create topology yet
SELECT ST_AsText(the_geom) 
   FROM hutt_pax.my_pgr_createSegmentedNetwork('hutt_pax.tmp_roads_in_buffer', 'hutt_pax.tmp_roads_in_buffer_20m', 'geom', 20.00);
	
/*
-- Generate catchment areas using segmented road
-- Query results ...
SELECT * 
  INTO hutt_pax.stop_catchment_20m_wellington
  FROM hutt_pax.my_pgr_createCatchmentArea(
  	'SELECT 
	      stop_id::integer AS id, 
	      the_geom::geometry AS the_geom
	FROM gtfs_stops',
  	'hutt_pax.roads_nz_wellington_20m'
  );
*/


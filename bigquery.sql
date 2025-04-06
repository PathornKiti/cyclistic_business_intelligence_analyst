-- Location Query Setting
WITH station_zip_distances AS (
  SELECT
    s.station_id,
    s.name AS station_name,
    s.latitude AS station_lat,
    s.longitude AS station_lon,
    z.zip_code,
    z.state_name,
    z.area_land_meters,
    z.area_water_meters,
    ST_DISTANCE(
      ST_GEOGPOINT(s.longitude, s.latitude),
      ST_GEOGPOINT(z.longitude, z.latitude)
    ) AS distance_meters
  FROM
    `conicle-ai.Recommend.citibike_stations` s
  JOIN
    `conicle-ai.Recommend.zip_code_boundaries` z
  ON ST_DWITHIN(
    ST_GEOGPOINT(s.longitude, s.latitude),
    ST_GEOGPOINT(z.longitude, z.latitude),
    2000
  )
),

nearest_zip_per_station AS (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY distance_meters ASC) AS row_num
    FROM station_zip_distances
  )
  WHERE row_num = 1
)

SELECT
  z.station_id,
  z.station_name,
  z.station_lat,
  z.station_lon,
  z.zip_code,
  n.Borough,
  n.Neighborhood,
  z.area_land_meters,
  z.area_water_meters
FROM
  nearest_zip_per_station z
INNER JOIN
  `conicle-ai.Recommend.neighborhood` n
ON
  CAST(z.zip_code AS STRING) = CAST(n.ZipCode AS STRING)

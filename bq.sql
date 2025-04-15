-- Denormalize Table
-- Step 1: Filter summer trips from target years
WITH summer_trips AS (
  SELECT *
  FROM `conicle-ai.Recommend.citibike_trips_external`
  WHERE EXTRACT(YEAR FROM starttime) IN (2014, 2015, 2016)
),

-- Step 2: Join trip data with station info and enrich with time/day info
joined_data AS (
  SELECT
    t.usertype,
    t.start_station_name,
    t.end_station_name,
    DATE(t.starttime) AS start_date,

    -- Time of day grouping
    CASE
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 5 AND 11 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 12 AND 16 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 17 AND 20 THEN 'Evening'
      ELSE 'Night'
    END AS time_of_day,

    ROUND(t.tripduration / 60, 2) AS trip_minute,

    -- Coordinates as string for mapping
    CONCAT(CAST(t.start_station_latitude AS STRING), ',', CAST(t.start_station_longitude AS STRING)) AS start_point,
    CONCAT(CAST(t.end_station_latitude AS STRING), ',', CAST(t.end_station_longitude AS STRING)) AS end_point,

    -- Borough and neighborhood info
    s_start.borough AS start_borough,
    s_start.neighborhood AS start_neighborhood,
    s_end.borough AS end_borough,
    s_end.neighborhood AS end_neighborhood
  FROM summer_trips t
  JOIN `conicle-ai.Recommend.citibike_stations_location` s_start
    ON ROUND(t.start_station_latitude, 3) = ROUND(s_start.latitude, 3)
       AND ROUND(t.start_station_longitude, 3) = ROUND(s_start.longitude, 3)
  JOIN `conicle-ai.Recommend.citibike_stations_location` s_end
    ON ROUND(t.end_station_latitude, 3) = ROUND(s_end.latitude, 3)
       AND ROUND(t.end_station_longitude, 3) = ROUND(s_end.longitude, 3)
),

-- Step 3: Join with weather and add season info
joined_with_weather AS (
  SELECT
    jd.*,

    -- Calendar enrichments
    EXTRACT(DAYOFWEEK FROM jd.start_date) AS day_of_week,
    EXTRACT(MONTH FROM jd.start_date) AS month,
    EXTRACT(YEAR FROM jd.start_date) AS year,

    -- Season classification based on month
    CASE
      WHEN EXTRACT(MONTH FROM jd.start_date) IN (6, 7, 8) THEN 'Summer'
      WHEN EXTRACT(MONTH FROM jd.start_date) IN (9, 10, 11) THEN 'Fall'
      WHEN EXTRACT(MONTH FROM jd.start_date) IN (3, 4, 5) THEN 'Spring'
      ELSE 'Winter'
    END AS season,

    -- Weather data
    w.temperature,
    w.visibility,
    w.wind_speed,
    w.precipitation
  FROM joined_data jd
  LEFT JOIN `conicle-ai.Recommend.weather_summary` w
    ON jd.start_date = DATE(w.timestamp)
),

-- Step 4: Extract coordinates for distance calculation
trip_with_distance AS (
  SELECT *,
    -- Extract latitude and longitude from point strings
    CAST(SPLIT(start_point, ',')[OFFSET(0)] AS FLOAT64) AS start_lat,
    CAST(SPLIT(start_point, ',')[OFFSET(1)] AS FLOAT64) AS start_lon,
    CAST(SPLIT(end_point, ',')[OFFSET(0)] AS FLOAT64) AS end_lat,
    CAST(SPLIT(end_point, ',')[OFFSET(1)] AS FLOAT64) AS end_lon
  FROM joined_with_weather
),

-- Step 5: Compute distance and speed using ST_DISTANCE (BigQuery native)
final_output AS (
  SELECT *,
    -- Distance in km using GEOGRAPHY functions
    ST_DISTANCE(
      ST_GEOGPOINT(start_lon, start_lat),
      ST_GEOGPOINT(end_lon, end_lat)
    ) / 1000 AS trip_distance_km, -- Convert from meters to kilometers

    -- Average speed = distance / duration (in hours)
    ROUND(
      (ST_DISTANCE(
        ST_GEOGPOINT(start_lon, start_lat),
        ST_GEOGPOINT(end_lon, end_lat)
      ) / 1000) / (trip_minute / 60), 2
    ) AS trip_avg_speed_kmh
  FROM trip_with_distance
)

-- Final result set
SELECT *
FROM final_output;




-- Station Congestion Table
WITH trips_started AS (
  SELECT
    start_station_name AS station_name,
    DATE(starttime) AS trip_date,
    COUNT(*) AS trips_started
  FROM
    `conicle-ai.Recommend.citibike_trips_external`
  GROUP BY
    station_name, trip_date
),

trips_ended AS (
  SELECT
    end_station_name AS station_name,
    DATE(stoptime) AS trip_date,
    COUNT(*) AS trips_ended
  FROM
    `conicle-ai.Recommend.citibike_trips_external`
  GROUP BY
    station_name, trip_date
),

net_flow AS (
  SELECT
    COALESCE(s.station_name, e.station_name) AS station_name,
    COALESCE(s.trip_date, e.trip_date) AS trip_date,
    IFNULL(e.trips_ended, 0) AS trips_ended,
    IFNULL(s.trips_started, 0) AS trips_started,
    IFNULL(e.trips_ended, 0) - IFNULL(s.trips_started, 0) AS net_flow
  FROM
    trips_started s
  FULL OUTER JOIN
    trips_ended e
  ON
    s.station_name = e.station_name AND s.trip_date = e.trip_date
)

SELECT
  nf.trip_date,
  nf.station_name,
  sl.borough,
  sl.neighborhood,
  sl.latitude,
  sl.longitude,
  sl.capacity,
  nf.trips_started,
  nf.trips_ended,
  nf.net_flow,

  -- 🚲 Utilization Metric
  SAFE_DIVIDE(nf.trips_started + nf.trips_ended, sl.capacity) AS trips_per_dock,

  -- ⚖️ Flow Stress Score
  SAFE_DIVIDE(ABS(nf.net_flow), sl.capacity) AS flow_stress_score,

  -- 🏷️ Utilization Status (optional flag column)
  CASE
    WHEN SAFE_DIVIDE(nf.trips_started + nf.trips_ended, sl.capacity) > 15 THEN 'Overused'
    WHEN SAFE_DIVIDE(nf.trips_started + nf.trips_ended, sl.capacity) < 6 THEN 'Underused'
    ELSE 'Efficient'
  END AS utilization_status

FROM
  net_flow nf
LEFT JOIN
  `conicle-ai.Recommend.citibike_stations_location` sl
ON
  nf.station_name = sl.name
WHERE
  sl.borough IS NOT NULL
  AND nf.trip_date BETWEEN '2014-01-01' AND '2016-12-31'
ORDER BY
  nf.trip_date, nf.station_name;


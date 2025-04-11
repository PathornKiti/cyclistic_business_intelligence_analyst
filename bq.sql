WITH summer_trips AS (
  SELECT *
  FROM `conicle-ai.Recommend.citibike_trips_external`
  WHERE EXTRACT(YEAR FROM starttime) IN (2014, 2015, 2016)
),

joined_data AS (
  SELECT
    t.usertype,
    DATE(t.starttime) AS start_date,
    CASE
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 5 AND 11 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 12 AND 16 THEN 'Afternoon'
      WHEN EXTRACT(HOUR FROM t.starttime) BETWEEN 17 AND 20 THEN 'Evening'
      ELSE 'Night'
    END AS time_of_day,
    CAST(t.tripduration / 60 AS INT64) AS trip_minute, 
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

joined_with_weather AS (
  SELECT
    jd.usertype,
    jd.start_borough,
    jd.end_borough,
    jd.start_neighborhood,
    jd.end_neighborhood,
    jd.start_date,
    jd.time_of_day,
    EXTRACT(DAYOFWEEK FROM jd.start_date) AS day_of_week,
    EXTRACT(MONTH FROM jd.start_date) AS month,
    EXTRACT(YEAR FROM jd.start_date) AS year,
    jd.trip_minute,
    w.temperature,
    w.visibility,
    w.wind_speed,
    w.precipitation
  FROM joined_data jd
  LEFT JOIN `conicle-ai.Recommend.weather_summary` w
    ON jd.start_date = DATE(w.timestamp)
)

SELECT *
FROM joined_with_weather;

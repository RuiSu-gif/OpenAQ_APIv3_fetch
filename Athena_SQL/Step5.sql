INSERT INTO openaq_db.daily_aq_stats (
    location_id,
    city,
    lat,
    lon,
    measure_date,
    pm25_daily_avg,
    o3_mda8,
    o3_max_1h,
    unit,
    year
)
WITH filtered AS (
    SELECT
        location_id,
        location,
        datetime,
        lat,
        lon,
        parameter,
        units,
        value,
        year,
        month
    FROM openaq_db.openaqMeasurements
    WHERE year = '2023'
      AND parameter IN ('pm25', 'o3')
      AND value >= 0
      AND try(from_iso8601_timestamp(datetime)) IS NOT NULL
),
o3_window AS (
    SELECT
        location_id,
        datetime,
        avg(value) OVER (
            PARTITION BY location_id
            ORDER BY try(from_iso8601_timestamp(datetime))
            ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS rolling_avg_o3
    FROM filtered
    WHERE parameter = 'o3'
)
SELECT
    f.location_id,
    arbitrary(f.location) AS city,
    arbitrary(f.lat) AS lat,
    arbitrary(f.lon) AS lon,
    date(try(from_iso8601_timestamp(f.datetime))) AS measure_date,
    avg(CASE WHEN f.parameter = 'pm25' THEN f.value END) AS pm25_daily_avg,
    max(o.rolling_avg_o3) AS o3_mda8,
    max(CASE WHEN f.parameter = 'o3' THEN f.value END) AS o3_max_1h,
    arbitrary(f.units) AS unit,
    f.year
FROM filtered f
LEFT JOIN o3_window o
  ON f.location_id = o.location_id
 AND f.datetime = o.datetime
GROUP BY
    f.location_id,
    date(try(from_iso8601_timestamp(f.datetime))),
    f.year;

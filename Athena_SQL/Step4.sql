CREATE EXTERNAL TABLE IF NOT EXISTS openaq_db.daily_aq_stats (
    location_id     int,
    city            string,
    lat             float,
    lon             float,
    measure_date    date,
    pm25_daily_avg  float,
    o3_mda8         float,
    o3_max_1h       float,
    unit            string,
    year            string
)
STORED AS PARQUET
LOCATION 's3://YOUR-BUCKET/openaq/daily_aq_stats/';
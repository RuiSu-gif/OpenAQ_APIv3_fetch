SELECT
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
FROM openaq_db.daily_aq_stats
WHERE year = '2023'
  AND measure_date >= DATE '2023-01-01'
  AND measure_date <=  DATE '2023-12-31';

SELECT
  sensor_name,
  sensor_type,
  COUNT(*) AS rows_in_long_table
FROM iceberg.metropt_quality_iceberg.dwd_metropt_sensor_long
GROUP BY sensor_name, sensor_type
ORDER BY sensor_name;

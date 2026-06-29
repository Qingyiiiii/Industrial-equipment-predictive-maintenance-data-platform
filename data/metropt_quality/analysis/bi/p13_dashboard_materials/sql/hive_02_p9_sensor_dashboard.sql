USE metropt_quality;
SELECT
  sensor_name,
  sensor_type,
  unit,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM vw_pbi_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;

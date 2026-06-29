USE metropt_quality;
SELECT
  dt,
  operating_state,
  COUNT(*) AS minute_count,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count,
  AVG(failure_window_rate) AS avg_failure_window_rate,
  AVG(avg_oil_temperature) AS avg_oil_temperature,
  AVG(avg_motor_current) AS avg_motor_current
FROM vw_pbi_metropt_window_kpi
GROUP BY dt, operating_state
ORDER BY dt, operating_state
LIMIT 100;

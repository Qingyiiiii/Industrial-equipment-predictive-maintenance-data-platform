USE metropt_quality_olap;
SELECT
  dt,
  operating_state,
  minute_count,
  sample_count,
  failure_sample_count,
  avg_failure_window_rate,
  avg_oil_temperature,
  avg_motor_current
FROM p12_metropt_window_state_kpi
ORDER BY dt, operating_state
LIMIT 100;

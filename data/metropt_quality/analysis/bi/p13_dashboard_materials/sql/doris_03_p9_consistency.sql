USE metropt_quality_olap;
SELECT 'sensor_rows' AS metric, COUNT(*) AS value FROM dws_metropt_sensor_kpi
UNION ALL
SELECT 'sensor_sample_sum' AS metric, SUM(sample_count) AS value FROM dws_metropt_sensor_kpi
UNION ALL
SELECT 'window_state_rows' AS metric, COUNT(*) AS value FROM p12_metropt_window_state_kpi
UNION ALL
SELECT 'window_sample_sum' AS metric, SUM(sample_count) AS value FROM p12_metropt_window_state_kpi
UNION ALL
SELECT 'window_failure_sample_sum' AS metric, SUM(failure_sample_count) AS value FROM p12_metropt_window_state_kpi;

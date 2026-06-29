SELECT
  COUNT(*) AS window_rows,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count
FROM iceberg.metropt_quality_iceberg.dws_metropt_window_kpi;

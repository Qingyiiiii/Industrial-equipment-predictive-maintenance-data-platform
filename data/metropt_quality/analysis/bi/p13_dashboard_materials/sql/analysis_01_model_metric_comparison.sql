-- P13 BI semantic SQL for model-performance page.
-- Source artifact: data/metropt_quality/analysis/models/p10_model_metric_comparison.tsv
-- Boundary: this is analysis-artifact SQL for BI import, not Hive/Trino/Doris cluster evidence.

SELECT
  source_type,
  model_name,
  status,
  precision,
  recall,
  f1,
  pr_auc,
  false_alarms_per_day,
  lead_time_model,
  detected_windows,
  mean_lead_time_hours
FROM p10_model_metric_comparison
WHERE source_type IN ('csv_derived', 'warehouse_derived')
ORDER BY source_type, model_name;

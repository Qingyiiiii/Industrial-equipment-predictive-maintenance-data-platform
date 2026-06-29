# P9 Model Baseline Report

## Dataset and Target

- Feature source: `data/metropt_quality/analysis/models/p9_window_features_1min_warehouse.parquet`.
- Feature source type: `warehouse-derived`.
- Target: `pre_failure_24h`.
- Rows used after excluding failure and post-maintenance windows: `243592`.
- Feature count: `221`.
- Train rows: `138115`, positives `2435`.
- Validation rows: `33636`, positives `1440`.
- Test rows: `71841`, positives `1022`.

The target is a weak early-warning label derived from configured failure starts. It is not a manually verified row-level production alarm label.

## Time Split

- Train: before `2020-06-01 00:00:00`.
- Validation: `2020-06-01 00:00:00` to before `2020-07-01 00:00:00`.
- Test: from `2020-07-01 00:00:00`.
- Split strategy: chronological fixed cutoffs, no random split.
- Chronological split check: `PASS` - Train, validation, and test ranges are strictly chronological.
- Leakage check: `PASS`; label columns in features: `[]`.

## Metrics

- `numpy_logistic_regression`: precision `0.0024`, recall `0.1204`, F1 `0.0047`, PR-AUC `0.007497780097422789`, false alarms/day `808.0794`
- `robust_anomaly_score`: precision `0.0303`, recall `0.9481`, F1 `0.0587`, PR-AUC `0.12749640360177622`, false alarms/day `492.7619`
- `random_forest`: precision `0.0068`, recall `0.0284`, F1 `0.0110`, PR-AUC `0.008944782886849962`, false alarms/day `66.7778`
- `isolation_forest`: precision `0.0226`, recall `0.9658`, F1 `0.0442`, PR-AUC `0.0698019401494058`, false alarms/day `676.5714`

## Lead Time

- Model: `numpy_logistic_regression`.
- Detected test failure windows: `1`.
- Mean lead time hours: `16.933333333333334`.
- Lead time is not reported for Random Forest, Isolation Forest, or robust anomaly score in this run.

## Comparison Artifacts

- CSV reference metrics: `data/metropt_quality/analysis/models/p10_csv_reference_model_metrics.json`.
- Warehouse metrics copy: `data/metropt_quality/analysis/models/p10_warehouse_model_metrics.json`.
- Metric comparison TSV: `data/metropt_quality/analysis/models/p10_model_metric_comparison.tsv`.
- Metric comparison report: `data/metropt_quality/analysis/reports/p10_model_baseline_comparison_report.md`.

## Limitations

- This is a baseline only and must not be described as a production predictive-maintenance model.
- Labels come from failure windows and pre-failure windows, so they are weak labels.
- Warehouse-derived features are now validated against ODS/DWD/DWS parity, but online scoring is still not integrated into Flink.

## Figures

- `data/metropt_quality/analysis/figures/p9_baseline_confusion_matrices.png`
- `data/metropt_quality/analysis/figures/p9_logistic_feature_weights.png`
- `data/metropt_quality/analysis/figures/p9_risk_score_timeline.png`

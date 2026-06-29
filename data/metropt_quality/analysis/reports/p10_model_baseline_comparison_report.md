# P10 Model Baseline Comparison Report

## Scope

- Target: `pre_failure_24h`.
- CSV-derived feature source: `data/metropt_quality/analysis/models/p9_window_features_1min.parquet`.
- Warehouse-derived feature source: `data/metropt_quality/analysis/models/p9_window_features_1min_warehouse.parquet`.
- Official updated metrics: `data/metropt_quality/analysis/models/p9_model_metrics.json`.
- Warehouse metrics copy: `data/metropt_quality/analysis/models/p10_warehouse_model_metrics.json`.
- CSV reference metrics: `data/metropt_quality/analysis/models/p10_csv_reference_model_metrics.json`.
- Comparison TSV: `data/metropt_quality/analysis/models/p10_model_metric_comparison.tsv`.

The warehouse-derived baseline uses the same fixed chronological split as P9. No random split is used.

## Split And Leakage Checks

| Source | Split check | Leakage check | Feature count |
| --- | --- | --- | ---: |
| CSV-derived | PASS | PASS | 221 |
| warehouse-derived | PASS | PASS | 221 |

## Metric Comparison

| Source | Model | Status | Precision | Recall | F1 | PR-AUC | False alarms/day | Lead time model | Detected windows | Mean lead time hours | Reason |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| csv_derived | isolation_forest | trained | 0.02426741057 | 0.9618395303 | 0.04734041272 | 0.07645180419 | 627.3650794 |  |  |  |  |
| csv_derived | numpy_logistic_regression | trained | 0.002410252391 | 0.1203522505 | 0.004725861605 | 0.007497791449 | 808.0793651 | numpy_logistic_regression | 1 | 16.93333333 |  |
| csv_derived | random_forest | trained | 0.006148440931 | 0.02739726027 | 0.01004304161 | 0.00859284761 | 71.84126984 |  |  |  |  |
| csv_derived | robust_anomaly_score | trained | 0.03026895324 | 0.9481409002 | 0.05866505222 | 0.1274964894 | 492.7619048 |  |  |  |  |
| warehouse_derived | isolation_forest | trained | 0.02263190479 | 0.9657534247 | 0.0442273654 | 0.06980194015 | 676.5714286 |  |  |  |  |
| warehouse_derived | numpy_logistic_regression | trained | 0.002410252391 | 0.1203522505 | 0.004725861605 | 0.007497780097 | 808.0793651 | numpy_logistic_regression | 1 | 16.93333333 |  |
| warehouse_derived | random_forest | trained | 0.006846081209 | 0.02837573386 | 0.01103081019 | 0.008944782887 | 66.77777778 |  |  |  |  |
| warehouse_derived | robust_anomaly_score | trained | 0.03026895324 | 0.9481409002 | 0.05866505222 | 0.1274964036 | 492.7619048 |  |  |  |  |
| warehouse_minus_csv | isolation_forest | delta | -0.001635505776 | 0.003913894325 | -0.003113047321 | -0.006649864038 | 49.20634921 |  |  |  |  |
| warehouse_minus_csv | numpy_logistic_regression | delta | 0 | 0 | 0 | -1.13513622e-08 | 0 | numpy_logistic_regression | 0 | 0 |  |
| warehouse_minus_csv | random_forest | delta | 0.0006976402776 | 0.0009784735812 | 0.0009877685871 | 0.0003519352764 | -5.063492063 |  |  |  |  |
| warehouse_minus_csv | robust_anomaly_score | delta | 0 | 0 | 0 | -8.576477303e-08 | 0 |  |  |  |  |

## RF / IF Availability

- Random Forest and Isolation Forest both produced trained metrics in this run.

## Lead Time Boundary

Lead time is computed only from `numpy_logistic_regression` predictions in both CSV-derived and warehouse-derived runs. Random Forest, Isolation Forest, and robust anomaly score metrics are compared on precision/recall/F1/PR-AUC/false alarms per day, but their lead time is not reported in this run.

## Decision

The warehouse-derived model baseline is accepted only if the chronological split check and leakage check are `PASS`, and skipped RF/IF models have explicit reasons. If warehouse metrics diverge from CSV-derived metrics, the comparison rows should be reviewed before promoting the model baseline.

# P10 Warehouse Feature Quality Report

## Scope

- Run id: `20260606_220741`.
- Overall status: `PASS`.
- Warehouse feature table: `data/metropt_quality/analysis/models/p9_window_features_1min_warehouse.parquet`.
- Parity report: `data/metropt_quality/analysis/reports/p9_feature_parity_report.md`.

This report validates the P10 warehouse-derived P9 feature table. It preserves the P9 CSV-derived feature artifact and treats parity differences as explicit PASS/WARN/FAIL checks.

## Warehouse Sources

| Source | Path | Rows |
| --- | --- | ---: |
| dwd_sensor_long | `hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long` | 22754220 |
| dws_sensor_kpi | `hdfs:///lakehouse/projects/metropt_quality/dws/sensor_kpi` | 15 |
| dws_window_kpi | `hdfs:///lakehouse/projects/metropt_quality/dws/window_kpi` | 269991 |
| ods_readings | `hdfs:///lakehouse/projects/metropt_quality/ods/readings` | 1516948 |

## Checks

| Check | Status | Detail |
| --- | --- | --- |
| p10_artifact_presence | PASS | Warehouse feature table, sample, parity JSON, and parity report exist. |
| warehouse_feature_metadata | PASS | Warehouse feature table has 252720 rows and 507 columns. |
| warehouse_feature_columns | PASS | Required label, minute, analog, pressure-delta, state, and rolling feature groups are present. |
| ods_to_feature_minute_alignment | PASS | Warehouse feature rows match ODS distinct event_minute count. |
| dws_to_ods_sample_alignment | PASS | DWS window sample_count sum matches ODS row count. |
| sensor_layer_alignment | PASS | DWD sensor_long and DWS sensor KPI both expose 15 sensors. |
| csv_warehouse_parity | PASS | CSV-derived and warehouse-derived feature references match without warnings. |
| warehouse_label_distribution | PASS | Labels are populated: failure_window=4959, pre_failure_24h=4897, normal_candidate=238695. |
| warehouse_model_feature_leakage | PASS | Candidate model features exclude labels and RUL fields; candidate_feature_count=497. |

## Leakage Boundary

- Candidate model features exclude `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds`.
- Label fields stay in the warehouse feature table only for evaluation, parity, and report slicing.

## Remaining Warnings

Warnings are acceptable only when they are explainable and do not imply feature-table corruption or label leakage. A P10 model rerun should use this warehouse-derived table only after this report is `PASS` or `PASS_WITH_WARNINGS`.

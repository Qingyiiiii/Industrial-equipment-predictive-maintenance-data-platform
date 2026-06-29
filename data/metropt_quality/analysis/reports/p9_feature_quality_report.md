# P9 Feature Quality Report

## Scope

- Role: Worker node offline data engineer.
- Run id: `20260606_master_local`.
- Overall status: `PASS_WITH_WARNINGS`.
- Feature table: `data/metropt_quality/analysis/models/p9_window_features_1min.parquet`.
- Feature rows/columns: `252720` / `507`.
- Log path: `data/metropt_quality/analysis/logs/20260606_master_local/07_p9_feature_quality_check.log`.

This report validates local P9 feature artifacts. It does not prove master Spark/Hive/ODS/DWD/DWS parity; those items remain 待 cluster 验证.

## Input Status

| Config key | Status | Path | Upstream producer |
| --- | --- | --- | --- |
| `input_csv` | OK | `<WORKER_PROJECT_ROOT>/datas/MetroPT3_AirCompressor.csv` | python src/00_metropt_preflight.py |
| `profile_dir` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/profile` | spark-submit src/01_metropt_profile.py |
| `ods_readings_parquet` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/ods/readings` | spark-submit src/02_metropt_csv_to_parquet.py |
| `dwd_sensor_long` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/dwd/sensor_long` | spark-submit src/03_metropt_dwd_sensor_long.py |
| `dws_overall_kpi` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/dws/overall_kpi` | spark-submit src/04_metropt_kpi_calc.py |
| `dws_window_kpi` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/dws/window_kpi` | spark-submit src/04_metropt_kpi_calc.py |
| `dws_sensor_kpi` | MISSING | `<WORKER_PROJECT_ROOT>/data/metropt_quality/dws/sensor_kpi` | spark-submit src/04_metropt_kpi_calc.py |

## Checks

| Check | Status | Detail |
| --- | --- | --- |
| p9_artifact_presence | PASS | Checked 22 required P9 artifacts; all exist and are non-empty. |
| feature_table_metadata | PASS | Feature table has 252720 rows and 507 columns, matching EDA summary. |
| feature_group_coverage | PASS | Minute, rolling, pressure-delta, digital activity, and state-transition feature groups are present. |
| model_feature_leakage | PASS | Model feature list excludes P9 labels and RUL fields. |
| feature_dictionary_leakage_flag | PASS | Feature dictionary marks labels and RUL as leakage-risk target/grouping fields. |
| chronological_split | PASS | Train, validation, and test ranges are strictly chronological. |
| trained_model_metrics | PASS | Trained fallback baseline models (numpy_logistic_regression, robust_anomaly_score) include precision, recall, F1, PR-AUC, false alarms/day, and confusion matrix. Skipped sklearn models: isolation_forest: scikit-learn training failed: PermissionError: [WinError 5] 拒绝访问。; random_forest: scikit-learn training failed: PermissionError: [WinError 5] 拒绝访问。. |
| local_full_analysis_inputs | WARN | ODS/DWD/DWS Parquet inputs are missing locally; P9 CSV-derived artifacts require master parity validation. |

## Artifact Manifest View

| Artifact | Type | Exists | Bytes |
| --- | --- | ---: | ---: |
| `data/metropt_quality/analysis/reports/p9_sensor_dictionary.md` | report | yes | 3806 |
| `data/metropt_quality/analysis/reports/p9_label_system.md` | report | yes | 4510 |
| `data/metropt_quality/analysis/reports/p9_eda_report.md` | report | yes | 4701 |
| `data/metropt_quality/analysis/reports/p9_feature_dictionary.md` | report | yes | 1698 |
| `data/metropt_quality/analysis/reports/p9_model_baseline_report.md` | report | yes | 2478 |
| `data/metropt_quality/analysis/models/p9_label_summary.json` | model_or_metadata | yes | 1208 |
| `data/metropt_quality/analysis/models/p9_label_summary.tsv` | model_or_metadata | yes | 349 |
| `data/metropt_quality/analysis/models/p9_feature_eda_summary.json` | model_or_metadata | yes | 28217 |
| `data/metropt_quality/analysis/models/p9_feature_dictionary.tsv` | model_or_metadata | yes | 12064 |
| `data/metropt_quality/analysis/models/p9_window_features_1min.parquet` | model_or_metadata | yes | 276426009 |
| `data/metropt_quality/analysis/models/p9_window_features_1min_sample.tsv` | model_or_metadata | yes | 3312776 |
| `data/metropt_quality/analysis/models/p9_model_metrics.json` | model_or_metadata | yes | 17068 |
| `data/metropt_quality/analysis/models/p9_logistic_feature_weights.tsv` | model_or_metadata | yes | 14899 |
| `data/metropt_quality/analysis/models/p9_model_prediction_sample.tsv` | model_or_metadata | yes | 92072 |
| `data/metropt_quality/analysis/figures/p9_daily_sample_failure_trend.png` | figure | yes | 162075 |
| `data/metropt_quality/analysis/figures/p9_pre_failure_sensor_delta.png` | figure | yes | 100250 |
| `data/metropt_quality/analysis/figures/p9_sensor_correlation_heatmap.png` | figure | yes | 184581 |
| `data/metropt_quality/analysis/figures/p9_pressure_current_oil_fault_timeline.png` | figure | yes | 455905 |
| `data/metropt_quality/analysis/figures/p9_state_transition_frequency.png` | figure | yes | 151426 |
| `data/metropt_quality/analysis/figures/p9_baseline_confusion_matrices.png` | figure | yes | 48926 |
| `data/metropt_quality/analysis/figures/p9_logistic_feature_weights.png` | figure | yes | 170326 |
| `data/metropt_quality/analysis/figures/p9_risk_score_timeline.png` | figure | yes | 185294 |

## Offline Engineering Notes

- `src/00` to `src/06` were reviewed as P0-P8 main-chain scripts; no P9 change was made to the accepted main chain.
- P9 feature generation remains in `analysis/`, so it does not overwrite ODS/DWD/DWS/Hive/Iceberg outputs.
- Standard `python -m compileall src analysis` is blocked by existing `__pycache__` replace permissions on this local copy. Syntax was validated with `compile(...)` without writing `.pyc`.
- Local full-analysis Parquet inputs are missing in this local copy. Existing P9 features were generated directly from the full CSV and should be regenerated or compared on master.
- The large `p9_window_features_1min.parquet` file is reproducible and may be omitted from cloud sync if size is a concern.

## Cluster Validation Required

- Run or compare `analysis/05_p9_feature_engineering.py` on master after ODS/DWD/DWS parity is confirmed.
- Check whether P9 features should remain CSV-derived analysis artifacts or be rebuilt from master ODS/DWS Parquet.
- If master has a working scikit-learn runtime, rerun `analysis/06_p9_model_experiments.py` to train Random Forest and Isolation Forest baselines; otherwise keep them explicitly skipped with the recorded reason.
- Keep all cluster/Hive/Spark conclusions as pending until master writes final validation evidence.

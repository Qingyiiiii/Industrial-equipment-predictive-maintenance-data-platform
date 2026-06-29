# P9 QA Checklist

## Scope

This checklist separates local documentation/analysis checks from cluster validation. It is for P9 BI / QA / documentation evidence review and should be read together with `p9_master_validation_checklist.md`.

## Evidence Levels

| Level | Meaning | Acceptable evidence | Current P9 state |
| --- | --- | --- | --- |
| Code static check | Python files parse or compile without executing cluster services. | Local compile command and return code. | Local check required for each delivery. |
| Local data check | CSV-derived or local artifact check completed locally. | Command, run id, output path, report. | P9 CSV-derived reports exist. |
| Local full analysis check | Local ODS/DWD/DWS Parquet exists and analysis reads those outputs. | `analysis/00_validate_analysis_inputs.py` returns all required inputs OK. | Not achieved; ODS/DWD/DWS Parquet missing locally. |
| Cluster check | Spark/Hive/Trino/Doris/Kafka/Flink/Redis or HDFS validation on the cluster. | Run path, logs, return code, acceptance summary. | 待 cluster 验证. |

## Local Evidence Check

| Area | Check | Expected result | Current status | Evidence |
| --- | --- | --- | --- | --- |
| Required context | P9 reports and validation evidence reviewed. | BI/QA work references existing P9 reports instead of duplicating modeling work. | PASS | This delivery uses the current `data/metropt_quality/analysis/reports/p9_*.md` report set. |
| Document paths | New P9 BI/QA documents exist under `data/metropt_quality/analysis/reports/`. | All paths exist and are non-empty. | PASS | `p9_portfolio_summary.md`, `p9_dashboard_field_dictionary.md`, `p9_qa_checklist.md`, `p9_master_validation_checklist.md`. |
| P8 boundary | P8 delivery package is not overwritten. | No edit to `data/metropt_quality/delivery_packages/p8_delivery_package_20260606_011332/`. | PASS | P9 docs are separate extension artifacts. |
| P9 report linkage | Index and route docs reference P9 outputs. | Root docs contain P9 extension entries. | PASS | `README.md`, `data/metropt_quality/analysis/reports/README.md`. |
| Label wording | Failure windows are described as weak labels. | No wording claims row-level true fault labels. | PASS | P9 label and QA docs preserve the weak-label boundary. |
| Model wording | Baseline metrics are not framed as production quality. | No production predictive-maintenance claim. | PASS | P9 portfolio and model sections say baseline only. |
| Split discipline | Time split is documented. | No random split recommendation. | PASS | Train/validation/test split documented as chronological. |
| Leakage control | Label/RUL fields are not model features. | Label fields are target/grouping only. | PASS | `p9_feature_quality_report.md` reports leakage check PASS. |
| Query samples | Hive/Trino/Doris SQL is prepared without claiming execution. | Samples are marked 待 cluster 验证. | PASS | `p9_dashboard_field_dictionary.md`. |
| Local full inputs | Local ODS/DWD/DWS Parquet is available. | All analysis input paths exist. | WARN | `analysis_input_validation.json` records missing local Parquet inputs. |
| Cluster claims | Local reports avoid cluster-pass language. | Cluster checks remain 待 cluster 验证. | PASS | `p9_master_validation_checklist.md` and delivery pending list. |

## Master Review Checklist

| Area | Master check | Expected result | Evidence to archive |
| --- | --- | --- | --- |
| P7 readiness | Run `bin/p7_ops_snapshot.sh`. | Readiness is PASS/READY or WARN with explicit resource reason. | `validation_runs/p7_ops_snapshot_<run_id>/ops_snapshot.md`. |
| P9 code syntax | Run `python -m compileall analysis`. | Return code `0`, or documented permission-related alternative. | Console output or log file. |
| P9 artifact QA | Run `python analysis/07_p9_feature_quality_check.py`. | `overall_status` is `PASS` or justified `PASS_WITH_WARNINGS`. | `p9_feature_quality_report.md`, `p9_feature_quality_checks.json`. |
| Feature regeneration | Run `python analysis/05_p9_feature_engineering.py` if master wants to regenerate. | Feature rows/columns match or differences are explained. | Updated P9 feature report and log. |
| Model rerun | Run `python analysis/06_p9_model_experiments.py`. | Chronological split remains; metrics include precision, recall, F1, PR-AUC, false alarms/day, lead time. | Updated `p9_model_metrics.json` and report. |
| sklearn models | If scikit-learn exists, confirm Random Forest / Isolation Forest status. | Models either train successfully or skip reason is explicit. | Updated model report. |
| Hive BI views | Run sample Hive queries from `p9_dashboard_field_dictionary.md`. | Queries return expected fields and non-empty offline results. | Hive query logs. |
| Trino / Doris samples | Run prepared samples only if extended query mode is started. | Query output and return code recorded. | Trino/Doris logs under validation run. |
| P9 delivery decision | Decide whether P9 artifacts enter a future formal package. | Decision records included/excluded paths and reason. | Master acceptance note or future delivery package index. |

## QA Rules

- Do not count `15,169,480 data points` as CSV rows; the current ODS row baseline is `1,516,948`.
- Do not use random train/test split for MetroPT-3 time series.
- Do not report accuracy alone.
- Do not promote P9 feature table to accepted DWS/Hive production output without master approval.
- Do not treat missing local ODS/DWD/DWS Parquet as a cluster failure.
- Do not treat local checks as cluster validation.

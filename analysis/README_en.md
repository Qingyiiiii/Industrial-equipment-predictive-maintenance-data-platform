# analysis Modeling And Evaluation

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

`analysis/` handles MetroPT-3 data quality analysis, multidimensional EDA, weak labels, feature engineering, baseline modeling, P9/P10 warehouse alignment, and report generation.

## Recommended Execution

Check inputs first:

```bash
python analysis/00_validate_analysis_inputs.py
```

Run the early analysis pipeline:

```bash
python analysis/run_metropt_analysis.py
```

P9/P10 single-step scripts are usually called by P14. For manual reproduction, run them in filename order.

For documentation review or demo preflight, if P10 artifacts already exist, use the fast path:

```bash
python analysis/08_p10_warehouse_feature_builder.py --reuse-existing
python analysis/09_p10_warehouse_feature_quality_check.py
python analysis/10_p10_warehouse_model_baseline.py --reuse-existing
python analysis/11_model_explainability_summary.py
```

`--reuse-existing` reuses existing artifacts only. It does not replace a formal P14 standard rerun.

Output directories:

```text
data/metropt_quality/analysis/reports/
data/metropt_quality/analysis/figures/
data/metropt_quality/analysis/models/
data/metropt_quality/analysis/logs/
```

## Files

| File | Purpose | Input | Output |
| --- | --- | --- | --- |
| `analysis_common.py` | Shared path, config, Spark, and report helpers | Imported by scripts | Not intended to run directly |
| `00_validate_analysis_inputs.py` | Checks whether Raw/Profile/ODS/DWD/DWS upstream inputs exist | Config and data directories | `analysis_input_validation*.json` |
| `01_data_quality_analysis.py` | ODS data quality analysis | ODS Parquet | `metropt_data_quality_report.md/json`, figures |
| `02_multidim_analysis.py` | Multidimensional analysis and charts | ODS/DWD/DWS | `metropt_multidim_analysis_report.md/json`, figures |
| `03_model_baseline.py` | Early baseline model from DWS window KPI | DWS window KPI | `metropt_baseline_model_report.md`, metrics, figures |
| `p9_common.py` | Shared P9 label, feature, and model helpers | Imported by P9/P10 scripts | Not intended to run directly |
| `04_p9_label_builder.py` | Builds sensor dictionary and weak-label documentation | CSV / configured failure windows | `p9_sensor_dictionary.md`, `p9_label_system.md` |
| `05_p9_feature_engineering.py` | Builds P9 EDA, minute features, and feature dictionary | CSV-derived data | `p9_window_features_1min.parquet`, P9 reports and figures |
| `06_p9_model_experiments.py` | Time-split baseline experiments | P9 minute features | `p9_model_metrics.json`, `p9_model_baseline_report.md` |
| `07_p9_feature_quality_check.py` | Checks existing P9 feature artifacts without rerunning heavy jobs | P9 reports/models | `p9_feature_quality_report.md`, checks JSON |
| `08_p10_warehouse_feature_builder.py` | Rebuilds warehouse-derived P9 features from ODS/DWD/DWS | Accepted Parquet | `p9_window_features_1min_warehouse.parquet`, parity report |
| `09_p10_warehouse_feature_quality_check.py` | Checks warehouse-derived features and leakage boundary | P10 feature artifacts | `p10_warehouse_feature_quality_report.md` |
| `10_p10_warehouse_model_baseline.py` | Reruns baseline on warehouse-derived features and compares with CSV-derived features | P9/P10 features | `p10_model_metric_comparison.*`, model report |
| `11_model_explainability_summary.py` | Produces an explainability summary from existing metrics and logistic weights; no new training | P9/P10 model artifacts | `p11_model_explainability_summary.json/md` |
| `run_metropt_analysis.py` | Runs early `00 -> 03` analysis tasks | Upstream Parquet | `analysis_run_summary.tsv`, step logs |

## How To Read Key Results

| Result | Focus |
| --- | --- |
| `analysis/reports/*.md` | Conclusions and boundaries; Chinese versions use `*.zh.md` |
| `analysis/figures/*.png` | Trends, correlations, model signals, and failure-window comparisons |
| `analysis/models/*.json` | Metrics, feature summaries, and quality checks |
| `analysis/models/*.parquet` | P9/P10 minute feature tables; do not open large files manually |
| `analysis/logs/<run_id>/analysis_run_summary.tsv` | Return code for each analysis step |

## Key Metrics

| Metric | Meaning |
| --- | --- |
| `precision` / `recall` / `f1` | Baseline classification metrics |
| `pr_auc` | More informative than ROC in imbalanced weak-label settings |
| `lead_time` | Warning lead time; must be tied to a specific model |
| `false alarms per day` | False-alarm intensity for interpretability |
| `risk_score` | Real-time signal-proxy output, not a production model probability |
| `PASS/WARN/SKIP/FAIL` | Validation status; must not be edited manually |

## Figure Reading Guide

| Figure | Focus |
| --- | --- |
| `daily_sample_count_trend.png` | Daily sample stability |
| `sensor_correlation_heatmap.png` | Sensor correlation and redundancy |
| `failure_window_sensor_contrast.png` | Sensor differences around failure windows |
| `baseline_confusion_matrices.png` | Baseline model error structure |
| `p9_risk_score_timeline.png` | Risk score over time |
| `p9_logistic_feature_weights.png` | Main Logistic Regression feature directions |

## Extension Path

| Document | Purpose |
| --- | --- |
| `data/metropt_quality/analysis/reports/p11_model_explainability_summary.md` | Explains current weak-label baseline metrics, feature weights, and production boundary |
| `data/metropt_quality/analysis/reports/rul_anomaly_extension_plan.md` | Describes how RUL regression and anomaly detection can be extended on top of P10 features |

## Common Issues

| Symptom | Action |
| --- | --- |
| Missing input | Run offline `src/run_metropt_offline.py`, or inspect `00_validate_analysis_inputs.py` output |
| Metrics look too good | Check time split and leakage notes; random split must not be presented as predictive evidence |
| RF/IF skipped | Read dependency/resource boundary notes; skipped steps are not failures by themselves |
| P9 and P10 differ | Check `p9_feature_parity_report.md`; warehouse-derived and CSV-derived features may have explainable differences |
| P10 rerun is slow | `--reuse-existing` is acceptable for review; formal validation still needs standard P14 |
| Chinese and English reports differ | English originals are evidence artifacts; Chinese versions are reader guides; key validation conclusions should match run directories |


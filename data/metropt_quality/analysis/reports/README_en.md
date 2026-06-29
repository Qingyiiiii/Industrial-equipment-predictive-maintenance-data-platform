# analysis/reports Report Index

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

This directory stores analysis, modeling, P9-P14 validation, and BI material reports. English originals are kept, and Chinese guide files are consistently named `*.zh.md`.

Reading principles:

1. For quick understanding, start with `.zh.md`.
2. For evidence verification, return to the English original and the related run directory.
3. When you see `pending`, `SKIP`, or `WARN`, also check later coverage reports such as `p9_p11_legacy_status_overlay_20260608.zh.md`.

## Recommended Reading Order

| Order | English original | Chinese guide | Purpose |
| --- | --- | --- | --- |
| 1 | `p9_phase_closure_20260607.md` | `p9_phase_closure_20260607.zh.md` | P9 closure boundary |
| 2 | `p9_master_validation_result_20260606.md` | `p9_master_validation_result_20260606.zh.md` | P9 cluster validation result |
| 3 | `p9_sensor_dictionary.md` | `p9_sensor_dictionary.zh.md` | Sensor dictionary |
| 4 | `p9_label_system.md` | `p9_label_system.zh.md` | Weak-label system |
| 5 | `p9_eda_report.md` | `p9_eda_report.zh.md` | P9 EDA |
| 6 | `p9_feature_dictionary.md` | `p9_feature_dictionary.zh.md` | P9 feature dictionary |
| 7 | `p9_model_baseline_report.md` | `p9_model_baseline_report.zh.md` | P9 baseline model |
| 8 | `p10_warehouse_feature_quality_report.md` | `p10_warehouse_feature_quality_report.zh.md` | P10 warehouse-derived feature quality |
| 9 | `p10_model_baseline_comparison_report.md` | `p10_model_baseline_comparison_report.zh.md` | P10 model comparison |
| 10 | `p11_realtime_risk_scoring_validation_report.md` | `p11_realtime_risk_scoring_validation_report.zh.md` | P11 realtime risk scoring validation |
| 11 | `p12_query_layer_validation_report.md` | `p12_query_layer_validation_report.zh.md` | P12 Trino/Doris query-layer validation |
| 12 | `p13_bi_dashboard_portfolio.md` | `p13_bi_dashboard_portfolio.zh.md` | P13 BI dashboard materials |
| 13 | `p14_master_validation_report_20260607_054200.md` | `p14_master_validation_report_20260607_054200.zh.md` | P14 one-command cluster validation |
| 14 | `p9_p11_legacy_status_overlay_20260608.md` | `p9_p11_legacy_status_overlay_20260608.zh.md` | Historical pending coverage |
| 15 | `p14_master_validation_report_20260609_020821.md` | `p14_master_validation_report_20260609_020821.zh.md` | Current latest standard P14 validation, `pass=18 warn=0 skip=0 fail=0` |

## Report Types

| Type | Files |
| --- | --- |
| Early analysis | `metropt_data_quality_report.*`, `metropt_multidim_analysis_report.*`, `metropt_baseline_model_report.*` |
| P9 data understanding | `p9_sensor_dictionary.*`, `p9_label_system.*`, `p9_eda_report.*` |
| P9 features and model | `p9_feature_dictionary.*`, `p9_feature_quality_report.*`, `p9_model_baseline_report.*` |
| P10 warehouse-sourced features | `p9_feature_parity_report.*`, `p10_warehouse_feature_quality_report.*`, `p10_model_baseline_comparison_report.*` |
| Realtime and query | `p11_realtime_risk_scoring_validation_report.*`, `p12_query_layer_validation_report.*` |
| BI materials | `p13_bi_dashboard_portfolio.*`, `p13_bi_field_semantics.*`, `p13_bi_query_evidence.*` |
| Validation and boundaries | `p9_master_validation_*`, `p14_master_validation_*`, `p9_p11_legacy_status_overlay_*` |

## Notes

- `.zh.md` files are Chinese reading guides and do not replace English originals.
- English originals and run directories are evidence artifacts.
- 2026-06-09 `p14_master_validation_20260609_020821` is the current latest formal standard P14 evidence, with status `PASS`.
- The 2026-06-08 `PASS_WITH_WARNINGS` result is historical evidence only and must not be used as the latest current status.
- `flink_signal_proxy_not_production_model` must not be described as a production ML model.


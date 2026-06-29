# analysis/reports 报告索引

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

本目录保存分析、建模、P9-P14 验收和 BI 素材报告。英文原文保留，中文说明文件统一命名为 `*.zh.md`。

阅读原则：

1. 想快速理解，先看 `.zh.md`。
2. 想核验证据，回到英文原文和对应 run_dir。
3. 看到 `pending`、`SKIP`、`WARN` 时，要同时查看后续覆盖报告，例如 `p9_p11_legacy_status_overlay_20260608.zh.md`。

## 推荐阅读顺序

| 顺序 | 英文原文 | 中文说明 | 用途 |
| --- | --- | --- | --- |
| 1 | `p9_phase_closure_20260607.md` | `p9_phase_closure_20260607.zh.md` | P9 收口边界 |
| 2 | `p9_master_validation_result_20260606.md` | `p9_master_validation_result_20260606.zh.md` | P9 cluster 复验结果 |
| 3 | `p9_sensor_dictionary.md` | `p9_sensor_dictionary.zh.md` | 传感器字典 |
| 4 | `p9_label_system.md` | `p9_label_system.zh.md` | 弱标签体系 |
| 5 | `p9_eda_report.md` | `p9_eda_report.zh.md` | P9 EDA |
| 6 | `p9_feature_dictionary.md` | `p9_feature_dictionary.zh.md` | P9 feature dictionary |
| 7 | `p9_model_baseline_report.md` | `p9_model_baseline_report.zh.md` | P9 baseline model |
| 8 | `p10_warehouse_feature_quality_report.md` | `p10_warehouse_feature_quality_report.zh.md` | P10 warehouse-derived feature 质量 |
| 9 | `p10_model_baseline_comparison_report.md` | `p10_model_baseline_comparison_report.zh.md` | P10 模型对比 |
| 10 | `p11_realtime_risk_scoring_validation_report.md` | `p11_realtime_risk_scoring_validation_report.zh.md` | P11 实时风险评分验收 |
| 11 | `p12_query_layer_validation_report.md` | `p12_query_layer_validation_report.zh.md` | P12 Trino/Doris 查询层复验 |
| 12 | `p13_bi_dashboard_portfolio.md` | `p13_bi_dashboard_portfolio.zh.md` | P13 BI 看板素材 |
| 13 | `p14_master_validation_report_20260607_054200.md` | `p14_master_validation_report_20260607_054200.zh.md` | P14 一键 cluster validation |
| 14 | `p9_p11_legacy_status_overlay_20260608.md` | `p9_p11_legacy_status_overlay_20260608.zh.md` | 旧 pending 覆盖表 |
| 15 | `p14_master_validation_report_20260609_020821.md` | `p14_master_validation_report_20260609_020821.zh.md` | 当前最新 standard P14 正式复验，`pass=18 warn=0 skip=0 fail=0` |

## 报告类型

| 类型 | 文件 |
| --- | --- |
| 早期分析 | `metropt_data_quality_report.*`、`metropt_multidim_analysis_report.*`、`metropt_baseline_model_report.*` |
| P9 数据理解 | `p9_sensor_dictionary.*`、`p9_label_system.*`、`p9_eda_report.*` |
| P9 特征模型 | `p9_feature_dictionary.*`、`p9_feature_quality_report.*`、`p9_model_baseline_report.*` |
| P10 数仓源特征 | `p9_feature_parity_report.*`、`p10_warehouse_feature_quality_report.*`、`p10_model_baseline_comparison_report.*` |
| 实时和查询 | `p11_realtime_risk_scoring_validation_report.*`、`p12_query_layer_validation_report.*` |
| BI 素材 | `p13_bi_dashboard_portfolio.*`、`p13_bi_field_semantics.*`、`p13_bi_query_evidence.*` |
| 复验和边界 | `p9_master_validation_*`、`p14_master_validation_*`、`p9_p11_legacy_status_overlay_*` |

## 注意

- `.zh.md` 是中文导读，不覆盖英文原文。
- 英文原文和 run_dir 是证据原件。
- 2026-06-09 `p14_master_validation_20260609_020821` 是当前最新正式 standard P14 证据，状态为 `PASS`。
- 2026-06-08 `PASS_WITH_WARNINGS` 只能作为历史证据，不能继续写成当前最新状态。
- `flink_signal_proxy_not_production_model` 不能写成生产 ML 模型。

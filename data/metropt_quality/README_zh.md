# data/metropt_quality 结果目录说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

`data/metropt_quality/` 保存 MetroPT-3 的本地运行结果、分析报告、图表、模型、logs、validation runs、交付报告和 delivery packages。

这个目录不是源代码入口。看代码请回到：

- `src/README.md`
- `streaming/README.md`
- `analysis/README.md`
- `bin/README.md`

## 子目录用途

| 目录 | 用途 | 先看什么 |
| --- | --- | --- |
| `analysis/` | EDA、特征、模型、BI 素材和报告 | `analysis/reports/`、`analysis/figures/`、`analysis/models/` |
| `logs/` | 离线 `src/run_metropt_offline.py` 每次运行日志 | `<run_id>/offline_run_summary.tsv` |
| `p1_logs/` | P1 offline/realtime acceptance 日志 | `<run_id>/summary.tsv` |
| `realtime_logs/` | 实时 Flink job pid/log | `flink_metropt_realtime.log` |
| `validation_runs/` | P2-P14 验收脚本输出，是排错和复验最重要目录 | `<run_dir>/summary.tsv` |
| `delivery_reports/` | P4 项目交付报告 | `delivery_report.md`、`delivery_summary.json` |
| `delivery_packages/` | P8 最终交付包 | `p8_delivery_package_20260606_011332/delivery_index.md` |

## log 文件怎么看

先不要从大日志开始。推荐顺序：

1. `summary.tsv`：看是否有 `FAIL`，再看 `WARN`、`SKIP`。
2. `*_steps.tsv`：如果是 P14，定位具体失败步骤。
3. `command.tsv`：确认当时参数和 mode。
4. 对应 `.log`：看第一处 error、return code、SQL rc。
5. `readiness.tsv`：如果是 P7，判断是否服务不可用或资源余量低。

常见状态含义：

| 状态 | 含义 |
| --- | --- |
| `PASS` | 该步骤通过 |
| `WARN` | 有风险或边界提示，不一定失败 |
| `SKIP` | 显式跳过，不能冒充完整 PASS |
| `FAIL` | 失败，必须看日志并重新运行 |
| `PASS_WITH_WARNINGS` | 业务链路通过，但有 WARN 边界 |

## analysis 结果怎么看

### reports

`analysis/reports/` 放 Markdown、JSON 和 TSV 报告。英文原文保留，中文说明使用 `*.zh.md`。

报告索引：

```text
analysis/reports/README.md
```

重点入口：

| 文件 | 说明 |
| --- | --- |
| `p9_phase_closure_20260607.md` / `.zh.md` | P9 收口边界 |
| `p9_p11_legacy_status_overlay_20260608.md` / `.zh.md` | 旧 pending 与后续关闭证据 |
| `p10_warehouse_feature_quality_report.md` / `.zh.md` | P10 warehouse-derived feature 质量 |
| `p11_realtime_risk_scoring_validation_report.md` / `.zh.md` | P11 实时风险评分验收 |
| `p12_query_layer_validation_report.md` / `.zh.md` | Trino/Doris 查询层复验 |
| `p13_bi_dashboard_portfolio.md` / `.zh.md` | BI 看板素材 |
| `p14_master_validation_report_20260607_054200.md` / `.zh.md` | P14 一键复验证据 |

### figures

`analysis/figures/*.png` 是分析图表。阅读方向：

| 图片 | 展示内容 |
| --- | --- |
| `daily_sample_count_trend.png` | 每日样本量趋势，用于发现缺口或采样异常 |
| `sensor_correlation_heatmap.png` | 传感器相关性，用于识别冗余和联动 |
| `failure_window_sensor_contrast.png` | 故障窗口与非故障窗口传感器差异 |
| `pressure_current_temperature_timeseries.png` | 压力、电流、油温时序联动 |
| `baseline_confusion_matrices.png` | baseline model 混淆矩阵 |
| `random_forest_feature_signals.png` | Random Forest 重要特征 |
| `p9_pressure_current_oil_fault_timeline.png` | P9 故障窗口时序解释 |
| `p9_sensor_correlation_heatmap.png` | P9 传感器相关性 |
| `p9_logistic_feature_weights.png` | Logistic Regression 特征权重 |
| `p9_risk_score_timeline.png` | P9 risk_score 时间线 |

### models

`analysis/models/` 保存特征表、metrics、质量检查和样例。

| 类型 | 说明 |
| --- | --- |
| `.parquet` | P9/P10 minute feature table，通常由 `analysis/05_*` 或 `analysis/08_*` 生成 |
| `.json` | metrics、quality checks、feature summary，适合快速查看结论 |
| `.tsv` / `.csv` | 样例、特征权重、指标对比，可直接打开查看 |

关键文件：

| 文件 | 来源模块 | 说明 |
| --- | --- | --- |
| `p9_window_features_1min.parquet` | `05_p9_feature_engineering.py` | CSV-derived P9 minute features |
| `p9_window_features_1min_warehouse.parquet` | `08_p10_warehouse_feature_builder.py` | warehouse-derived P9 minute features |
| `p9_model_metrics.json` | `06_p9_model_experiments.py` / `10_p10_warehouse_model_baseline.py` | 当前模型指标 |
| `p10_model_metric_comparison.tsv` | `10_p10_warehouse_model_baseline.py` | CSV-derived vs warehouse-derived 指标对比 |

## SQL 文件怎么看

SQL 主要在：

```text
analysis/bi/p13_dashboard_materials/sql/
validation_runs/*/sql/
```

阅读时看三点：

1. 查询引擎：`hive`、`trino`、`doris` 或 analysis artifact。
2. 输入对象：Hive table、Trino schema、Doris table 或本地 TSV。
3. 输出位置：`sample_outputs/`、`p12_query_results.tsv`、对应 `.log`。

P12 查询层复验重点不是单条 SQL 好看，而是 Hive vs Trino、Hive vs Doris 的一致性 `PASS`。

## delivery packages 怎么看

最终交付包入口：

```text
delivery_packages/p8_delivery_package_20260606_011332/delivery_index.md
```

交付包索引：

```text
delivery_packages/README.md
```

中文版本命名为 `*.zh.md`。推荐阅读：

1. `delivery_index.md` / `.zh.md`
2. `package_summary.md` / `.zh.md`
3. `project_overview.md` / `.zh.md`
4. `acceptance_results.md` / `.zh.md`
5. `run_order.md` / `.zh.md`
6. `troubleshooting_entry.md` / `.zh.md`

旧交付包 `p8_delivery_package_20260606_010634` 和 `p8_delivery_package_20260606_011100` 保留为历史证据，当前正式入口是 `p8_delivery_package_20260606_011332`。

## 常见问题

| 问题 | 回答 |
| --- | --- |
| `validation_runs` 很多，怎么看最新 | 按目录名时间戳排序，优先看本轮任务对应前缀 |
| `.log` 很长 | 先看 `summary.tsv` 锁定失败步骤，再看对应 log |
| `.parquet` 怎么打开 | 不建议手工打开大文件；用生成脚本或 sample TSV 查看 |
| `.png` 是否是最终看板 | 不是，它们是分析图表和 BI 素材 |
| 中文报告和英文原文哪个为准 | 英文原文和 run_dir 是证据原件，中文报告用于快速理解 |

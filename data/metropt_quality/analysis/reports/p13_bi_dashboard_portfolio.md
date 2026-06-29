# P13 BI Dashboard Portfolio

生成时间：2026-06-07

## 结论

P13 BI 看板素材已成型，结论：`PASS_WITH_BOUNDARIES`。

本材料包将 P9 文档、P10 warehouse-derived 模型结果、P11 实时风险样例、P12 Trino / Doris 查询结果整理为 5 个可展示看板页面。当前输出是 BI 作品集素材和口径说明，不是 BI 产品上线证明。

## 材料入口

| 类型 | 路径 |
| --- | --- |
| BI 看板说明 | `data/metropt_quality/analysis/reports/p13_bi_dashboard_portfolio.md` |
| 字段口径说明 | `data/metropt_quality/analysis/reports/p13_bi_field_semantics.md` |
| 查询与样例证据 | `data/metropt_quality/analysis/reports/p13_bi_query_evidence.md` |
| SQL 材料目录 | `data/metropt_quality/analysis/bi/p13_dashboard_materials/sql/` |
| 样例输出目录 | `data/metropt_quality/analysis/bi/p13_dashboard_materials/sample_outputs/` |

## 页面 1：总体健康

目标：说明 MetroPT-3 数据规模、运行状态分布、故障窗口占比和查询层证据状态。

| 图表 | 字段 | 数据来源 | SQL / 证据 | 样例输出 |
| --- | --- | --- | --- | --- |
| KPI strip：ODS / DWD / DWS 行数 | `ods_rows`, `rows_in_long_table`, `window_rows`, `sample_count` | Trino / Iceberg, Hive DWS | `sql/trino_01_p9_ods_count.sql`, `sql/trino_02_p9_sensor_long_counts.sql`, `sql/trino_03_p9_window_consistency.sql` | `sample_outputs/trino_trino_01_p9_ods_count.tsv`, `sample_outputs/trino_trino_02_p9_sensor_long_counts.tsv`, `sample_outputs/trino_trino_03_p9_window_consistency.tsv` |
| 日期-运行状态矩阵 | `dt`, `operating_state`, `minute_count`, `sample_count` | Hive `vw_pbi_metropt_window_kpi` | `sql/hive_01_p9_window_dashboard.sql` | `sample_outputs/hive_hive_01_p9_window_dashboard.tsv` |
| 查询层验收状态 | `engine`, `query_id`, `return_code`, `real_seconds`, `rows` | P12 validation run | `bin/p12_query_layer_validation.sh` | `sample_outputs/p12_query_results.tsv` |

页面脚注：

- `failure_window_rate` 来自配置故障窗口的弱标签统计，不是人工逐行标注。
- Trino / Doris 查询层证据来自 P12 新运行，不复用旧 P5 smoke 结果。

## 页面 2：传感器风险

目标：展示 15 个传感器的物理语义、类型、故障窗口占比和统计特征，帮助解释高风险信号来源。

| 图表 | 字段 | 数据来源 | SQL / 证据 | 样例输出 |
| --- | --- | --- | --- | --- |
| 传感器风险排行 | `sensor_name`, `sensor_type`, `failure_window_rate`, `sample_count`, `failure_sample_count` | Doris `metropt_quality_olap.dws_metropt_sensor_kpi` | `sql/doris_01_p9_sensor_dashboard.sql` | `sample_outputs/doris_doris_01_p9_sensor_dashboard.tsv` |
| 传感器均值 / 波动表 | `avg_sensor_value`, `std_sensor_value`, `unit` | Hive `vw_pbi_metropt_sensor_kpi` | `sql/hive_02_p9_sensor_dashboard.sql` | `sample_outputs/hive_hive_02_p9_sensor_dashboard.tsv` |
| Sensor long 覆盖检查 | `sensor_name`, `sensor_type`, `rows_in_long_table` | Trino / Iceberg `dwd_metropt_sensor_long` | `sql/trino_02_p9_sensor_long_counts.sql` | `sample_outputs/trino_trino_02_p9_sensor_long_counts.tsv` |

页面脚注：

- `failure_window_rate` 是按配置故障窗口聚合的解释字段，只能用于弱标签对比。
- 传感器物理语义以 `p9_sensor_dictionary.md` 为准；`DV_eletric` 在项目中标准化为 `dv_electric`。

## 页面 3：故障窗口

目标：展示配置故障窗口、预故障窗口、恢复窗口和运行状态聚合，解释“标签如何产生”。

| 图表 | 字段 | 数据来源 | SQL / 证据 | 样例输出 |
| --- | --- | --- | --- | --- |
| 标签规模概览 | `label`, `positive_rows`, `negative_rows`, `positive_rate` | P9 analysis `p9_label_summary.tsv` | `sql/analysis_02_label_summary.sql` | `sample_outputs/p9_label_summary.tsv` |
| 故障窗口状态聚合 | `dt`, `operating_state`, `failure_sample_count`, `avg_failure_window_rate` | Hive `vw_pbi_metropt_window_kpi` | `sql/hive_01_p9_window_dashboard.sql` | `sample_outputs/hive_hive_01_p9_window_dashboard.tsv` |
| Doris window-state 对照 | `dt`, `operating_state`, `sample_count`, `failure_sample_count` | Doris `p12_metropt_window_state_kpi` | `sql/doris_02_p9_window_dashboard.sql` | `sample_outputs/doris_doris_02_p9_window_dashboard.tsv` |

页面脚注：

- `failure_window`, `pre_failure_1h`, `pre_failure_6h`, `pre_failure_24h`, `post_maintenance` 均是配置区间推导的弱标签，不是人工逐行故障判定。
- `rul_seconds` 只允许作为候选目标或分析字段，不能进入模型特征。

## 页面 4：模型表现

目标：用 P10 warehouse-derived baseline 说明模型可复现，但不把 baseline 包装成生产预测模型。

| 图表 | 字段 | 数据来源 | SQL / 证据 | 样例输出 |
| --- | --- | --- | --- | --- |
| 模型指标对比 | `source_type`, `model_name`, `precision`, `recall`, `f1`, `pr_auc` | P10 analysis `p10_model_metric_comparison.tsv` | `sql/analysis_01_model_metric_comparison.sql` | `sample_outputs/p10_model_metric_comparison.tsv` |
| 误报负担 | `false_alarms_per_day`, `model_name`, `source_type` | P10 analysis | `sql/analysis_01_model_metric_comparison.sql` | `sample_outputs/p10_model_metric_comparison.tsv` |
| Lead time 摘要 | `lead_time_model`, `detected_windows`, `mean_lead_time_hours` | P10 analysis | `sql/analysis_01_model_metric_comparison.sql` | `sample_outputs/p10_model_metric_comparison.tsv` |

页面脚注：

- P10 使用固定 chronological split，不使用随机切分。
- Lead time 只归属 `numpy_logistic_regression`，不混写到 Random Forest、Isolation Forest 或 anomaly score。
- Random Forest 和 Isolation Forest 在本轮 master 环境已训练出指标，但仍是 baseline，不是生产模型。

## 页面 5：实时风险

目标：展示 P11 风险字段已经在线生成，并明确这是 Flink signal-proxy scorer，不是生产 ML model service。

| 图表 | 字段 | 数据来源 | SQL / 证据 | 样例输出 |
| --- | --- | --- | --- | --- |
| 最新风险状态 | `risk_score`, `risk_level`, `risk_reason`, `model_version`, `risk_score_source` | Redis `metropt_quality:risk:latest:compressor_1` | P11 acceptance script / Redis sample | `sample_outputs/p11_redis_risk_sample.log` |
| 风险事件样例 | `event_id`, `event_time`, `operating_state`, `risk_score`, `risk_level`, `risk_reason`, `model_version` | Hive `dws_metropt_realtime_risk_events` | `sql/hive_03_realtime_risk_sample.sql` | `sample_outputs/p11_hive_realtime_risk_sample.log` |
| 风险链路验收 | `run_id`, `pass`, `warn`, `fail`, `with_risk` | P11 / P6 validation runs | `bin/p11_realtime_risk_acceptance.sh`, `bin/p6_realtime_demo_mode.sh` | `p11_realtime_risk_scoring_validation_report.md` |

页面脚注：

- `risk_score_source=flink_signal_proxy_not_production_model` 必须在页面脚注中展示。
- `dws_metropt_realtime_risk_events` 是追加式验证表，多次 `earliest-offset` 验收可能产生重复 event_id，不能把累计行数当作业务唯一事件数。

## 全局展示边界

- 弱标签只能写作“configured failure-window weak label”，不能写作真实人工标注。
- P9/P10 模型只能写作 baseline / production-candidate evidence，不能写作生产预测维护模型。
- P11 当前是 signal-proxy online scoring，不是生产模型服务。
- P12 证明 Trino / Doris 查询层能执行 P9 字段字典 SQL 并与 Hive 关键口径一致，不证明 BI 产品已经上线。

## 验收清单

| 检查项 | 结论 |
| --- | --- |
| 固定 3 到 5 个看板页面 | PASS，固定为 5 页 |
| 每页绑定字段字典 | PASS，字段详见 `p13_bi_field_semantics.md` |
| 每页绑定 SQL 或 analysis artifact query | PASS，SQL 材料位于 `p13_dashboard_materials/sql/` |
| 每页绑定样例输出 | PASS，样例输出位于 `p13_dashboard_materials/sample_outputs/` |
| 标注 Hive / Trino / Doris / P9 analysis 数据来源 | PASS |
| 弱标签边界 | PASS，已写入页面脚注和全局边界 |
| dry-run / signal-proxy / 生产化边界 | PASS，已写入实时风险页和全局边界 |

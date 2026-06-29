# P13 BI Query And Sample Evidence

生成时间：2026-06-07

## 结论

P13 查询样例和样例输出证据已整理完成。当前材料使用“样例输出证据”，未生成独立 BI 工具截图；满足 P13 的“截图或样例输出”要求。

## 证据目录

```text
data/metropt_quality/analysis/bi/p13_dashboard_materials/
├── sql/
└── sample_outputs/
```

## P12 集群查询证据

P12 run_dir：

```text
/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423
```

P12 summary：

```text
SUMMARY pass=30 warn=0 skip=0 fail=0
```

| 页面 | SQL | 样例输出 | 引擎 | rc | rows | 说明 |
| --- | --- | --- | --- | ---: | ---: | --- |
| 总体健康 | `sql/hive_01_p9_window_dashboard.sql` | `sample_outputs/hive_hive_01_p9_window_dashboard.tsv` | Hive | 0 | 100 | 日期-运行状态聚合 |
| 传感器风险 | `sql/hive_02_p9_sensor_dashboard.sql` | `sample_outputs/hive_hive_02_p9_sensor_dashboard.tsv` | Hive | 0 | 15 | sensor KPI BI view |
| 总体健康 | `sql/trino_01_p9_ods_count.sql` | `sample_outputs/trino_trino_01_p9_ods_count.tsv` | Trino | 0 | 1 | ODS 行数 |
| 传感器风险 | `sql/trino_02_p9_sensor_long_counts.sql` | `sample_outputs/trino_trino_02_p9_sensor_long_counts.tsv` | Trino | 0 | 15 | DWD sensor-long 覆盖 |
| 总体健康 | `sql/trino_03_p9_window_consistency.sql` | `sample_outputs/trino_trino_03_p9_window_consistency.tsv` | Trino | 0 | 1 | DWS window KPI 汇总 |
| 传感器风险 | `sql/doris_01_p9_sensor_dashboard.sql` | `sample_outputs/doris_doris_01_p9_sensor_dashboard.tsv` | Doris | 0 | 15 | Doris sensor KPI 排行 |
| 故障窗口 | `sql/doris_02_p9_window_dashboard.sql` | `sample_outputs/doris_doris_02_p9_window_dashboard.tsv` | Doris | 0 | 100 | Doris window-state KPI |
| 总体健康 | `sql/doris_03_p9_consistency.sql` | `sample_outputs/doris_doris_03_p9_consistency.tsv` | Doris | 0 | 5 | Doris consistency metrics |

P12 汇总表：

| 文件 | 用途 |
| --- | --- |
| `sample_outputs/p12_query_results.tsv` | 查询 rc、耗时、返回行数、日志路径 |
| `sample_outputs/p12_consistency.tsv` | Hive vs Trino、Hive vs Doris 一致性 |

## P9 / P10 Analysis 证据

| 页面 | SQL / 查询材料 | 样例输出 | 来源 | 说明 |
| --- | --- | --- | --- | --- |
| 故障窗口 | `sql/analysis_02_label_summary.sql` | `sample_outputs/p9_label_summary.tsv` | P9 analysis | 标签规模和正例率；弱标签 |
| 模型表现 | `sql/analysis_01_model_metric_comparison.sql` | `sample_outputs/p10_model_metric_comparison.tsv` | P10 analysis | CSV-derived vs warehouse-derived baseline 指标 |

边界：

- `analysis_*.sql` 是 BI 导入 analysis artifact 后的语义 SQL，不是 Hive/Trino/Doris 集群复验 SQL。
- P10 模型指标来自 fixed chronological split 和 leakage check PASS 的 analysis artifact。
- Lead time 只归属 `numpy_logistic_regression`。

## P11 实时风险证据

| 页面 | SQL / 查询材料 | 样例输出 | 来源 | 说明 |
| --- | --- | --- | --- | --- |
| 实时风险 | `sql/hive_03_realtime_risk_sample.sql` | `sample_outputs/p11_hive_realtime_risk_sample.log` | Hive risk table | 风险事件字段样例 |
| 实时风险 | P11 Redis sample | `sample_outputs/p11_redis_risk_sample.log` | Redis latest-risk key | 最新风险状态样例 |

P11 关键字段：

```text
risk_score
risk_level
risk_reason
model_version
risk_score_source
model_feature_set_version
```

实时风险边界：

- `risk_score_source=flink_signal_proxy_not_production_model` 必须作为页面脚注展示。
- P11 证明风险字段在线生成，不证明生产 ML 模型服务上线。
- Hive risk table 是追加式验证表，不能把累计行数当业务唯一事件数。

## 页面到证据映射

| 页面 | 必备证据 |
| --- | --- |
| 总体健康 | `trino_01_p9_ods_count.sql`、`trino_03_p9_window_consistency.sql`、`hive_01_p9_window_dashboard.sql`、`p12_query_results.tsv` |
| 传感器风险 | `hive_02_p9_sensor_dashboard.sql`、`doris_01_p9_sensor_dashboard.sql`、`trino_02_p9_sensor_long_counts.sql` |
| 故障窗口 | `analysis_02_label_summary.sql`、`p9_label_summary.tsv`、`hive_01_p9_window_dashboard.sql`、`doris_02_p9_window_dashboard.sql` |
| 模型表现 | `analysis_01_model_metric_comparison.sql`、`p10_model_metric_comparison.tsv` |
| 实时风险 | `hive_03_realtime_risk_sample.sql`、`p11_hive_realtime_risk_sample.log`、`p11_redis_risk_sample.log` |

## 验收结论

- 每个页面均绑定字段来源和 SQL / 查询材料。
- 每个页面均绑定样例输出证据。
- Hive / Trino / Doris 页面使用 P12 集群运行证据，全部 `rc=0`。
- 模型表现页使用 P10 analysis artifact，不伪装成集群 SQL。
- 实时风险页使用 P11 Redis / Hive 样例，不伪装成生产模型服务。
- 弱标签页面明确标注 configured failure-window weak label，不包装成人工标注。

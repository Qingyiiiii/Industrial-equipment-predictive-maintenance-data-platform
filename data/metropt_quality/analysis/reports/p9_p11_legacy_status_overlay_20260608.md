# P9/P11 Legacy Report Status Overlay

生成日期：2026-06-08

用途：解释 P9/P11 历史报告中的 `pending` / `待 cluster 验证` 表述在后续 P10/P11/P12/P14 之后的当前状态。原始报告作为当时阶段证据保留，不直接改写为后验结论。

## 覆盖关系

| 历史报告 | 原始边界 | 当前状态 | 关闭证据 |
| --- | --- | --- | --- |
| `p9_master_validation_checklist.md` | Spark/Hive/Trino/Doris/Kafka/Flink/Redis 仍需 master 侧记录返回码和日志 | 已分阶段关闭主要 master 验证项 | P10 warehouse-derived 特征/模型、P11 实时风险、P12 查询层、P14 一键复验 |
| `p9_feature_quality_report.md` | cluster/Hive/Spark conclusions pending | P10 已从 ODS/DWD/DWS-derived 特征重建并完成质量检查 | `p10_warehouse_feature_quality_report.md`、P14 `p10_warehouse_feature_quality` |
| `p9_model_baseline_report.md` | P9 baseline 为 CSV-derived analysis artifact，不是生产模型 | 边界仍保留；P10 已增加 warehouse-derived baseline 复跑 | `p10_model_baseline_comparison_report.md`、P14 `p10_warehouse_model_baseline` |
| `p9_dashboard_field_dictionary.md` | Trino/Doris samples prepared but master execution pending | 已由 P12/P14 查询层复验关闭 | `p12_query_layer_validation_report.md`、P14 `p12_trino_doris_query_layer` |
| `p9_phase_closure_20260607.md` | Trino/Doris P9 samples pending extended-query validation | 已由 P12/P14 关闭 | P12 `SUMMARY pass=30 warn=0 skip=0 fail=0`，P14 查询层步骤 PASS |
| `p11_realtime_risk_scoring_validation_report.md` | P11 不覆盖 Trino/Doris 查询层 | 边界仍保留；查询层另由 P12/P14 证明 | `p12_query_layer_validation_report.md`、P14 cluster validation |

## 当前仍需保留的边界

- P9/P10 模型只能写作 baseline 或 production-candidate evidence，不能写作生产预测维护模型。
- P11 当前是 Flink signal-proxy online scoring，不是生产 ML model service。
- P13 是 BI 作品集素材和口径说明，不是 BI 产品上线证明。
- `dws_metropt_realtime_risk_events` 是追加式验证表，多次 replay 可能产生重复 `event_id`；不能用累计行数代表业务唯一事件数。
- P14 使用 `--skip-*` 产生的结果不能作为完整标准 PASS；skip 必须保留在报告中。

## 2026-06-08 完善阶段补充

验证负责人接手后重新执行当前时点复验：

| 项 | 结果 |
| --- | --- |
| P7 初始快照 | `p7_ops_snapshot_20260608_032959`，`SUMMARY pass=22 warn=19 skip=17 fail=0`；Hive/Kafka/Flink 未启动，Trino/Doris 未启动 |
| 第一次 P14 | `p14_master_validation_20260608_033247`，P12 因 `start_trino_hadoop2` 失败而 FAIL；业务查询本身后续均 PASS |
| P12 修复复跑 | 手动启动 hadoop2 Trino worker 后，`p12_query_layer_validation_20260608_043955` 全部 PASS |
| 第二次 P14 | `p14_master_validation_20260608_050123`，`final_status: PASS_WITH_WARNINGS`，所有业务步骤 PASS，唯一 WARN 为 P7 资源余量低 |

当前 WARN 说明：`offline_hive_spark` 核心服务已就绪，但 `hadoop1` 可用内存约 1506 MB / 12%。这是资源余量风险，不是功能失败；重离线任务前应先查看 YARN 运行应用并考虑停止 Trino/Doris。

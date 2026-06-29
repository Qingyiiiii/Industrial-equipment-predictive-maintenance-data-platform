# P12 Trino / Doris Query Layer Validation Report

生成时间：2026-06-07

## 结论

P12 Trino / Doris 扩展查询复验结论：`PASS`。

本轮已在 master 集群重新启动并验证 Trino extended query mode 和 Doris extended query mode，执行 P9 dashboard field dictionary 中的 Trino / Iceberg 与 Doris 查询样例，并用 Hive 同口径结果完成一致性对照。本报告是 P9 查询层复验证据入口；不复用 P5 旧 smoke 查询结果作为 P9 SQL 验收依据。

## 执行边界

| 项 | 内容 |
| --- | --- |
| 固定巡检入口 | `bin/p7_ops_snapshot.sh` |
| P7 快照 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p7_ops_snapshot_20260607_035011` |
| 基础服务启动 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/start_base_services_20260607_035246`，`SUMMARY pass=12 warn=0 skip=0 fail=0` |
| P12 脚本 | `bin/p12_query_layer_validation.sh` |
| P12 run_id | `20260607_035423` |
| P12 run_dir | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423` |
| P12 总结 | `SUMMARY pass=30 warn=0 skip=0 fail=0` |

P7 快照显示 HDFS/YARN 正常，Hive、Trino、Doris 初始未运行；因此先通过 `bin/start_base_services.sh --hive-count` 恢复 Hive 与基础服务，再执行 P12 查询层复验。

## 查询结果

| engine | query_id | rc | seconds | rows | log |
| --- | --- | ---: | ---: | ---: | --- |
| Hive | `hive_01_p9_window_dashboard` | 0 | 56 | 100 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/hive_hive_01_p9_window_dashboard.log` |
| Hive | `hive_02_p9_sensor_dashboard` | 0 | 17 | 15 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/hive_hive_02_p9_sensor_dashboard.log` |
| Trino | `trino_01_p9_ods_count` | 0 | 3 | 1 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/trino_trino_01_p9_ods_count.log` |
| Trino | `trino_02_p9_sensor_long_counts` | 0 | 2 | 15 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/trino_trino_02_p9_sensor_long_counts.log` |
| Trino | `trino_03_p9_window_consistency` | 0 | 2 | 1 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/trino_trino_03_p9_window_consistency.log` |
| Doris | `doris_01_p9_sensor_dashboard` | 0 | 0 | 15 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_doris_01_p9_sensor_dashboard.log` |
| Doris | `doris_02_p9_window_dashboard` | 0 | 0 | 100 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_doris_02_p9_window_dashboard.log` |
| Doris | `doris_03_p9_consistency` | 0 | 0 | 5 | `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_doris_03_p9_consistency.log` |

完整查询结果表：

```text
/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/p12_query_results.tsv
```

## Hive 口径一致性

| check | status | expected | actual | note |
| --- | --- | ---: | ---: | --- |
| `ods_rows_hive_vs_trino` | PASS | 1516948 | 1516948 | `ods_metropt_readings` |
| `dwd_sensor_long_rows_hive_vs_trino` | PASS | 22754220 | 22754220 | `dwd_metropt_sensor_long` |
| `dwd_sensor_long_groups_hive_vs_trino` | PASS | 15 | 15 | P9 sensor long grouped query shape |
| `dws_window_rows_hive_vs_trino` | PASS | 269991 | 269991 | `dws_metropt_window_kpi` |
| `dws_window_sample_sum_hive_vs_trino` | PASS | 1516948 | 1516948 | `dws_metropt_window_kpi` |
| `dws_window_failure_sum_hive_vs_trino` | PASS | 29960 | 29960 | `dws_metropt_window_kpi` |
| `sensor_rows_hive_vs_doris` | PASS | 15 | 15 | `dws_metropt_sensor_kpi` |
| `sensor_sample_sum_hive_vs_doris` | PASS | 22754220 | 22754220 | `dws_metropt_sensor_kpi` |
| `window_state_rows_hive_vs_doris` | PASS | 500 | 500 | `p12_metropt_window_state_kpi` |
| `window_sample_sum_hive_vs_doris` | PASS | 1516948 | 1516948 | `p12_metropt_window_state_kpi` |
| `window_failure_sum_hive_vs_doris` | PASS | 29960 | 29960 | `p12_metropt_window_state_kpi` |

完整一致性结果表：

```text
/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/p12_consistency.tsv
```

## Doris 装载对象

本轮 Doris 查询样例使用 P12 脚本从 Hive 导出并装载以下对象：

| Doris 对象 | 来源 | 用途 |
| --- | --- | --- |
| `metropt_quality_olap.dws_metropt_sensor_kpi` | Hive `metropt_quality.dws_metropt_sensor_kpi` | P9 sensor dashboard Doris SQL |
| `metropt_quality_olap.p12_metropt_window_state_kpi` | Hive `vw_pbi_metropt_window_kpi` 按 `dt, operating_state` 聚合 | P9 window dashboard Doris SQL 与一致性检查 |

装载日志：

- `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/export_hive_sensor_kpi_for_doris.log`
- `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/export_hive_window_state_for_doris.log`
- `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_create_p12_tables.log`
- `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_load_sensor_kpi.log`
- `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423/doris_load_window_state_kpi.log`

## 验收判定

- Trino 查询 rc：全部为 `0`。
- Doris 查询 rc：全部为 `0`。
- 查询结果与 Hive 样例口径一致：Trino 6 项一致性 PASS，Doris 5 项一致性 PASS。
- P9 字段字典中的 Trino / Doris 查询已由本轮 P12 新证据覆盖。
- 未将旧 P5 查询结果冒充 P9 SQL 复验。

## 边界

- P12 证明 Trino / Doris 查询层可以执行 P9 字段字典 SQL，并与 Hive 关键口径一致。
- P12 不声明 BI 产品已上线，不声明查询服务生产 SLA 已完成。
- Doris 中 `p12_metropt_window_state_kpi` 是本轮 P12 验收用映射表，不替代 Hive canonical DWS 表。
- Trino 查询基于 `iceberg.metropt_quality_iceberg` schema；后续如切换 catalog/schema，需要重新执行本报告对应脚本。

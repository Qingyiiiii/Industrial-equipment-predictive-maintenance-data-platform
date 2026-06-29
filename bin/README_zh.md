# bin 脚本说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

`bin/` 放置集群启动、巡检、验收、交付、排错辅助脚本。大部分脚本会生成 `summary.tsv`，用 `PASS/WARN/SKIP/FAIL` 表示结果。

运行前建议：

```bash
cd /home/common/tmp/pycharm_Design
source /etc/profile.d/bigdata.sh
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
```

## 怎么看脚本输出

| 文件 | 说明 |
| --- | --- |
| `summary.tsv` | 最重要，先看是否有 `FAIL`，再看 `WARN` 和 `SKIP` |
| `command.tsv` | 本次运行参数和命令入口 |
| `*_steps.tsv` | master validation 的步骤级状态 |
| `*.log` | 失败步骤的详细日志 |
| `readiness.tsv` | P7 的服务/资源就绪判断 |

`WARN` 不一定是失败，常见含义是资源余量低、可选组件未运行或历史边界提示。`SKIP` 不能冒充完整 PASS。

## 脚本索引

| 脚本 | 用途 | 典型输入 | 输出 |
| --- | --- | --- | --- |
| `p0_config_drift_check.sh` | 只读检查 JDK/Hadoop/Hive/Spark/Flink/Kafka/Trino/Doris 配置漂移 | `--hosts` | 控制台 summary |
| `p0_cluster_health_check.sh` | 检查基础服务和可选 extended 服务健康 | `--profile basic|full` | 健康检查 summary |
| `start_base_services.sh` | 启动缺失的 HDFS/YARN/PostgreSQL/Hive/Kafka/Redis/Flink | `--check-only`、`--restart` | `validation_runs/start_base_services_*` |
| `start_realtime_mode.sh` | 确保实时小闭环基础服务可用，不提交 Flink job | `--check-only`、`--hive-count` | `validation_runs/start_realtime_mode_*` |
| `start_extended_query_mode.sh` | 启动/检查 Trino 和 Doris extended query mode | `--trino-only`、`--doris-only`、`--allow-swapoff` | `validation_runs/start_extended_query_mode_*` |
| `metropt_hive_mr_count_check.sh` | 使用 Hive-on-MR 做 offline/realtime COUNT smoke | `--mode offline|realtime` | Hive COUNT 日志 |
| `ds_metropt_offline_00_04.sh` | DolphinScheduler 兼容入口，跑离线 `00 -> 04` | 无 | 离线 log |
| `ds_metropt_offline_full.sh` | DolphinScheduler 兼容入口，跑完整离线链路 | 无 | 离线 log 和 summary |
| `p1_config_backup.sh` | 备份三节点关键配置 | `--hosts`、`--backup-base` | 配置备份目录 |
| `p1_tmp_cleanup_plan.sh` | 生成 `/home/common/tmp` 清理候选，不默认删除 | `--dry-run|--apply`、`--days` | candidate manifest |
| `p1_metropt_offline_acceptance.sh` | P1 离线验收，覆盖 run、HDFS、Hive、可选 Trino | `--skip-run`、`--require-trino` | `data/metropt_quality/p1_logs/offline_*` |
| `p1_metropt_realtime_acceptance.sh` | P1 实时 KPI 验收 | `--max-events`、`--rate`、`--inject-dlq-test` | `data/metropt_quality/p1_logs/realtime_*` |
| `p2_resource_baseline.sh` | 只读采集资源基线 | `--mode basic|realtime|extended|all` | `validation_runs/resource_baseline_*` |
| `p2_query_perf_compare.sh` | Hive/Trino/Doris 查询性能对比，不启动服务 | `--engine`、`--query-set`、`--timeout` | `query_perf_results.tsv` |
| `p2_log_maintenance_plan.sh` | 日志清理候选计划，不默认删除 | `--dry-run|--apply` | candidate manifest |
| `p3_data_quality_check.sh` | 项目数据质量验收 | `--skip-spark`、`--skip-hive`、`--skip-realtime` | `validation_runs/p3_data_quality_*` |
| `p3_project_delivery_acceptance.sh` | 项目级交付验收，可选 full offline/realtime/query | `--run-offline-full`、`--run-realtime` | `validation_runs/p3_project_delivery_*` |
| `p4_delivery_report.sh` | 从 P3/P7 等证据生成交付报告 | `--p3-run`、`--output-root` | `delivery_reports/p4_delivery_*` |
| `p5_doris_acceptance.sh` | Doris 扩展查询闭环验收 | `--start`、`--load-kpi`、`--query-smoke` | `validation_runs/p5_doris_*` |
| `p6_realtime_demo_mode.sh` | 实时 demo，默认包含 P1 KPI 与 P11 risk | `--start`、`--status`、`--stop` | `validation_runs/p6_realtime_demo_*` |
| `p7_ops_snapshot.sh` | 只读运维快照，当前每次运行前的固定入口 | `--hosts` | `ops_snapshot.md/json`、`readiness.tsv` |
| `p7_alert_rules_plan.sh` | 生成告警阈值建议，不安装任何服务 | `--dry-run` | alert plan |
| `p8_build_delivery_package.sh` | 从最新证据构建 delivery package | `--output-root`、`--package-name` | `delivery_packages/p8_delivery_package_*` |
| `p10_p9_master_validation.sh` | P14 一键 master validation，当前正式复验入口 | `--mode smoke|standard|full` | `validation_runs/p14_master_validation_*` |
| `p11_realtime_risk_acceptance.sh` | P11 实时风险评分验收 | `--max-events`、`--inject-dlq-test` | `validation_runs/p11_realtime_risk_*` |
| `p12_query_layer_validation.sh` | P12 Trino/Doris 查询层复验和一致性检查 | `--allow-swapoff`、`--skip-start` | `validation_runs/p12_query_layer_validation_*` |
| `p1_common.sh` | P1 公共函数 | 被 P1 脚本引用 | 不单独运行 |
| `p2_common_ops.sh` | P2+ 公共函数，run_dir 和 summary 生成 | 被 P2+ 脚本引用 | 不单独运行 |

## 本地 Python 辅助脚本

这些脚本不启动集群，用于本地质量检查、作品集打包或演示样本准备。

| 脚本 | 用途 | 典型输入 | 输出 |
| --- | --- | --- | --- |
| `local_code_quality_check.py` | 本地 AST、unittest、Markdown 链接检查 | 无 | 控制台 `LOCAL_CODE_QUALITY=PASS` |
| `build_metropt_sample.py` | 从完整 CSV 生成 head 小样本，便于演示和 contract check | `--rows`、`--source`、`--output` | `data/metropt_quality/samples/metropt_sample_head_*csv` |
| `build_portfolio_package.py` | 构建精简作品集包，只复制文档、报告和演示入口 | `--output-root`、`--package-name` | `data/metropt_quality/delivery_packages/portfolio_final_*/delivery_index.md` |

## 推荐入口

本地轻量质量检查：

```powershell
python bin/local_code_quality_check.py
```

本地小样本：

```powershell
python bin/build_metropt_sample.py --rows 1000
```

作品集包：

```powershell
python bin/build_portfolio_package.py
```

日常开工：

```bash
bin/p7_ops_snapshot.sh
bin/p10_p9_master_validation.sh --mode smoke
```

正式复验：

```bash
bin/p10_p9_master_validation.sh --mode standard --allow-swapoff --realtime-max-events 1000 --realtime-wait-seconds 60 --query-timeout 300
```

查询层单独复验：

```bash
bin/p12_query_layer_validation.sh --allow-swapoff
```

实时链路单独复验：

```bash
bin/p6_realtime_demo_mode.sh --start --duration-minutes 0 --max-events 1000 --rate 500 --wait-seconds 60
```

## 报错后怎么看

1. 找本次 run_dir。
2. 先打开 `summary.tsv`。
3. 定位第一条 `FAIL`。
4. 打开同目录对应 `.log`。
5. 如果是 P14，继续看 `p14_steps.tsv` 的 `log`、`child_run_dir`、`next_action`。
6. 如果是 P7 WARN，打开 `readiness.tsv` 判断是否资源类 WARN。

不要手工把 `FAIL` 改成 `PASS`；必须修复后重新运行并保留新 run_dir。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| `offline_hive_spark` WARN | 多数是内存余量低，先看 P7 `host_metrics.tsv` 和 `readiness.tsv` |
| Doris 端口判断混乱 | 不要只看 `8040`，先确认监听进程；当前 Doris FE/BE 使用项目文档中的端口边界 |
| Kafka CLI 报 Java 版本 | Kafka 4.1.2 需要 JDK17，检查 `/export/server/jdk17` |
| Trino worker 启动慢 | 先看 `start_trino_hadoop*.log`，再看 P12 是否最终 SQL 和 consistency PASS |
| `SKIP` 出现在 P14 | 检查是否使用了 `--mode smoke` 或 `--skip-*`；有 SKIP 不能作为完整 standard PASS |

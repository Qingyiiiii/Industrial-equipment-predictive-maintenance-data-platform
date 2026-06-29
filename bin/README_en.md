# bin Script Guide

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

`bin/` contains scripts for cluster startup, inspection, validation, delivery, and troubleshooting support. Most scripts generate `summary.tsv` with `PASS/WARN/SKIP/FAIL` statuses.

Before running cluster scripts:

```bash
cd /home/common/tmp/pycharm_Design
source /etc/profile.d/bigdata.sh
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
```

## How To Read Script Outputs

| File | Meaning |
| --- | --- |
| `summary.tsv` | Most important file; check `FAIL` first, then `WARN` and `SKIP` |
| `command.tsv` | Runtime parameters and command entry |
| `*_steps.tsv` | Step-level status for master validation |
| `*.log` | Detailed log for failed steps |
| `readiness.tsv` | P7 service/resource readiness |

`WARN` is not necessarily failure. It often means low resource headroom, optional service not running, or a historical boundary note. `SKIP` cannot be presented as a complete PASS.

## Script Index

| Script | Purpose | Typical input | Output |
| --- | --- | --- | --- |
| `p0_config_drift_check.sh` | Read-only drift check for JDK/Hadoop/Hive/Spark/Flink/Kafka/Trino/Doris config | `--hosts` | Console summary |
| `p0_cluster_health_check.sh` | Checks base services and optional extended services | `--profile basic|full` | Health summary |
| `start_base_services.sh` | Starts missing HDFS/YARN/PostgreSQL/Hive/Kafka/Redis/Flink services | `--check-only`, `--restart` | `validation_runs/start_base_services_*` |
| `start_realtime_mode.sh` | Ensures services for the realtime loop are ready without submitting Flink jobs | `--check-only`, `--hive-count` | `validation_runs/start_realtime_mode_*` |
| `start_extended_query_mode.sh` | Starts or checks Trino and Doris extended query mode | `--trino-only`, `--doris-only`, `--allow-swapoff` | `validation_runs/start_extended_query_mode_*` |
| `metropt_hive_mr_count_check.sh` | Hive-on-MR offline/realtime COUNT smoke | `--mode offline|realtime` | Hive COUNT logs |
| `ds_metropt_offline_00_04.sh` | DolphinScheduler-compatible entry for offline `00 -> 04` | None | Offline log |
| `ds_metropt_offline_full.sh` | DolphinScheduler-compatible full offline entry | None | Offline log and summary |
| `p1_config_backup.sh` | Backs up key configs from three nodes | `--hosts`, `--backup-base` | Config backup directory |
| `p1_tmp_cleanup_plan.sh` | Generates `/home/common/tmp` cleanup candidates; does not delete by default | `--dry-run|--apply`, `--days` | Candidate manifest |
| `p1_metropt_offline_acceptance.sh` | P1 offline acceptance for run, HDFS, Hive, and optional Trino | `--skip-run`, `--require-trino` | `data/metropt_quality/p1_logs/offline_*` |
| `p1_metropt_realtime_acceptance.sh` | P1 realtime KPI acceptance | `--max-events`, `--rate`, `--inject-dlq-test` | `data/metropt_quality/p1_logs/realtime_*` |
| `p2_resource_baseline.sh` | Read-only resource baseline collection | `--mode basic|realtime|extended|all` | `validation_runs/resource_baseline_*` |
| `p2_query_perf_compare.sh` | Hive/Trino/Doris query performance comparison; does not start services | `--engine`, `--query-set`, `--timeout` | `query_perf_results.tsv` |
| `p2_log_maintenance_plan.sh` | Generates log cleanup candidates; does not delete by default | `--dry-run|--apply` | Candidate manifest |
| `p3_data_quality_check.sh` | Project data quality acceptance | `--skip-spark`, `--skip-hive`, `--skip-realtime` | `validation_runs/p3_data_quality_*` |
| `p3_project_delivery_acceptance.sh` | Project-level delivery acceptance with optional full offline/realtime/query | `--run-offline-full`, `--run-realtime` | `validation_runs/p3_project_delivery_*` |
| `p4_delivery_report.sh` | Builds delivery reports from P3/P7 evidence | `--p3-run`, `--output-root` | `delivery_reports/p4_delivery_*` |
| `p5_doris_acceptance.sh` | Doris extended-query loop acceptance | `--start`, `--load-kpi`, `--query-smoke` | `validation_runs/p5_doris_*` |
| `p6_realtime_demo_mode.sh` | Realtime demo, including P1 KPI and P11 risk by default | `--start`, `--status`, `--stop` | `validation_runs/p6_realtime_demo_*` |
| `p7_ops_snapshot.sh` | Read-only operations snapshot; standard entry before each run | `--hosts` | `ops_snapshot.md/json`, `readiness.tsv` |
| `p7_alert_rules_plan.sh` | Generates alert threshold suggestions without installing services | `--dry-run` | Alert plan |
| `p8_build_delivery_package.sh` | Builds delivery package from latest evidence | `--output-root`, `--package-name` | `delivery_packages/p8_delivery_package_*` |
| `p10_p9_master_validation.sh` | P14 one-command master validation; current formal validation entry | `--mode smoke|standard|full` | `validation_runs/p14_master_validation_*` |
| `p11_realtime_risk_acceptance.sh` | P11 realtime risk scoring acceptance | `--max-events`, `--inject-dlq-test` | `validation_runs/p11_realtime_risk_*` |
| `p12_query_layer_validation.sh` | P12 Trino/Doris query-layer validation and consistency check | `--allow-swapoff`, `--skip-start` | `validation_runs/p12_query_layer_validation_*` |
| `p1_common.sh` | Shared P1 functions | Imported by P1 scripts | Not intended to run directly |
| `p2_common_ops.sh` | Shared P2+ functions for run directories and summaries | Imported by P2+ scripts | Not intended to run directly |

## Local Python Helpers

These scripts do not start the cluster. They support local quality checks, packaging, or demo sample preparation.

| Script | Purpose | Typical input | Output |
| --- | --- | --- | --- |
| `local_code_quality_check.py` | Local AST, unittest, and Markdown link checks | None | Console `LOCAL_CODE_QUALITY=PASS` |
| `build_metropt_sample.py` | Builds a head sample from the full CSV for demo and contract checks | `--rows`, `--source`, `--output` | `data/metropt_quality/samples/metropt_sample_head_*csv` |
| `build_portfolio_package.py` | Builds a compact portfolio package with docs, reports, and demo entries only | `--output-root`, `--package-name` | `data/metropt_quality/delivery_packages/portfolio_final_*/delivery_index.md` |

## Recommended Entries

Local lightweight quality check:

```powershell
python bin/local_code_quality_check.py
```

Local sample:

```powershell
python bin/build_metropt_sample.py --rows 1000
```

Portfolio package:

```powershell
python bin/build_portfolio_package.py
```

Daily startup:

```bash
bin/p7_ops_snapshot.sh
bin/p10_p9_master_validation.sh --mode smoke
```

Formal validation:

```bash
bin/p10_p9_master_validation.sh --mode standard --allow-swapoff --realtime-max-events 1000 --realtime-wait-seconds 60 --query-timeout 300
```

Standalone query-layer validation:

```bash
bin/p12_query_layer_validation.sh --allow-swapoff
```

Standalone real-time validation:

```bash
bin/p6_realtime_demo_mode.sh --start --duration-minutes 0 --max-events 1000 --rate 500 --wait-seconds 60
```

## After An Error

1. Find the current run directory.
2. Open `summary.tsv` first.
3. Locate the first `FAIL`.
4. Open the related `.log` in the same directory.
5. For P14, inspect `p14_steps.tsv` fields such as `log`, `child_run_dir`, and `next_action`.
6. For P7 WARN, inspect `readiness.tsv` to determine whether it is a resource warning.

Do not manually rewrite `FAIL` to `PASS`; fix the issue, rerun, and keep the new run directory.

## Common Issues

| Symptom | Action |
| --- | --- |
| `offline_hive_spark` WARN | Usually low memory headroom; check P7 `host_metrics.tsv` and `readiness.tsv` |
| Doris port confusion | Do not rely on `8040` alone; confirm the listening process and project port boundaries |
| Kafka CLI Java version error | Kafka 4.1.2 needs JDK17; check `/export/server/jdk17` |
| Slow Trino worker startup | Check `start_trino_hadoop*.log`, then confirm P12 SQL and consistency status |
| `SKIP` appears in P14 | Check whether `--mode smoke` or `--skip-*` was used; SKIP cannot represent full standard PASS |


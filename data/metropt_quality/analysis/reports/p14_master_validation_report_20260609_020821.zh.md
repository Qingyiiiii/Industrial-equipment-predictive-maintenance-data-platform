# P14 Cluster Validation Report 中文摘要 20260609_020821

## 结论

- run_id: `20260609_020821`
- run_dir: `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p14_master_validation_20260609_020821`
- final_status: `PASS`
- summary: `pass=18 warn=0 skip=0 fail=0`
- mode: `standard`

本次运行是 2026-06-09 当前时点的正式 standard P14 复验，不是 smoke，也没有使用 `--skip-*` 跳过关键链路。该 run 可作为当前最新正式交付证据。

## 覆盖范围

| 模块 | 结论 | 说明 |
| --- | --- | --- |
| Python dependency / syntax | PASS | 依赖检查和静态语法检查通过 |
| P7 ops snapshot | PASS | 子 run `p7_ops_snapshot_20260609_020821`，`pass=41 warn=0 skip=17 fail=0` |
| start_base_services | PASS | 子 run `start_base_services_20260609_020841`，基础服务检查通过 |
| P10 warehouse feature builder | PASS | warehouse-derived P9 minute features 重建通过 |
| P10 feature quality | PASS | parity 和 leakage boundary 检查通过 |
| P10 warehouse model baseline | PASS | warehouse-derived baseline 重跑通过 |
| Hive dashboard SQL | PASS | window dashboard 返回 100 行，sensor dashboard 返回 15 行 |
| P6/P11 realtime demo | PASS | 子 run `p6_realtime_demo_20260609_021557`，实时 KPI 和 risk evidence 通过 |
| P12 Trino/Doris query layer | PASS | 子 run `p12_query_layer_validation_20260609_022304`，`child_pass=30 warn=0 skip=0 fail=0` |

## 关键耗时

| 步骤 | seconds |
| --- | ---: |
| p10_warehouse_feature_builder | 71 |
| p10_warehouse_model_baseline | 107 |
| ensure_realtime_services_ready | 121 |
| p6_realtime_demo_with_p11_risk | 426 |
| p12_trino_doris_query_layer | 365 |

## 边界

- 本次 standard P14 结果可替代 2026-06-08 `PASS_WITH_WARNINGS` 作为当前最新正式证据。
- 2026-06-08 `PASS_WITH_WARNINGS` 保留为历史证据；其 WARN 是资源余量提示，不是业务链路失败。
- 2026-06-09 smoke run `p14_master_validation_20260609_000132` 仍只作为快速检查证据，不能替代 standard。
- P11 内部 `dlq_inject_check` 未开启注入测试属于 P11 子脚本的可选测试提示；P14 汇总中 P6/P11 子链路为 `child_pass=10 warn=0 skip=0 fail=0`。

## 本地归档文件

| 文件 | 用途 |
| --- | --- |
| `p14_master_validation_report_20260609_020821.md` | 远端 `validation_report.md` 本地归档 |
| `p14_summary_20260609_020821.tsv` | 远端 `summary.tsv` 本地归档 |
| `p14_master_validation_steps_20260609_020821.tsv` | 远端 `p14_steps.tsv` 本地归档 |
| `p14_hive_results_20260609_020821.tsv` | 远端 `p14_hive_results.tsv` 本地归档 |

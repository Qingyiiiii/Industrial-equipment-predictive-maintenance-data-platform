# P14 cluster validation report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p14_master_validation_report_20260607_054200.md`

## 这份文档是什么

P14 一键 cluster validation 报告，记录 P9/P10/P11/P12 的自动化复验结果。

## 输入是什么

依赖检查、P7、基础服务、P10、Hive SQL、P6/P11、P12。

## 输出是什么

validation_report.md、summary.tsv、p14_steps.tsv、hive results。

## 怎么看

先看 final_status 和 summary，再看 p14_steps 是否全部 PASS。

## 关键术语

- `P14`
- `cluster validation`
- `summary.tsv`
- `p14_steps.tsv`
- `PASS`
- `WARN`
- `SKIP`
- `FAIL`

## 证据边界

这是 2026-06-07 的 clean PASS；2026-06-08 复验是历史 `PASS_WITH_WARNINGS`，当前最新正式 standard P14 已更新为 2026-06-09 `PASS`。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

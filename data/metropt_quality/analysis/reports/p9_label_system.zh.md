# P9 label system（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_label_system.md`

## 这份文档是什么

P9 弱标签体系说明，记录 failure windows、pre-failure、normal candidate 等规则。

## 输入是什么

MetroPT failure windows、CSV 时间序列。

## 输出是什么

label rules、label distribution、leakage notes。

## 怎么看

先看 Configured Failure Windows 和 Label Rules，再看 Time Split Requirement。

## 关键术语

- `P9`
- `weak label`
- `failure_window`
- `pre_failure`
- `normal_candidate`
- `rul_seconds`

## 证据边界

这些是弱标签，不是人工逐行真实标签。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

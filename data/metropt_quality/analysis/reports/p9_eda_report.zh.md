# P9 EDA report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_eda_report.md`

## 这份文档是什么

P9 深度 EDA 报告，说明传感器分布、故障窗口前后变化和业务信号。

## 输入是什么

MetroPT CSV、failure windows、P9 feature engineering 输出。

## 输出是什么

EDA 报告和 P9 图表。

## 怎么看

先看 Data Scope、Failure-Window Contrast、Business-Useful Signals。

## 关键术语

- `P9`
- `EDA`
- `failure window`
- `sensor contrast`
- `business signal`

## 证据边界

EDA 是解释性分析，不是模型验收。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

# P10 warehouse feature quality report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p10_warehouse_feature_quality_report.md`

## 这份文档是什么

P10 warehouse-derived feature 质量报告，说明从 accepted ODS/DWD/DWS 重建 P9 features 后是否通过质量检查。

## 输入是什么

ODS/DWD/DWS Parquet、P9 feature reference、quality check 脚本。

## 输出是什么

feature quality report、quality checks JSON、parity 结论。

## 怎么看

先看 Scope 和 Checks，再看 Remaining Warnings。重点是 row count、time range、label distribution 和 leakage。

## 关键术语

- `P10`
- `warehouse-derived`
- `feature quality`
- `parity`
- `leakage`
- `PASS`

## 证据边界

P10 证明数仓源特征可用，不代表模型算法升级。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

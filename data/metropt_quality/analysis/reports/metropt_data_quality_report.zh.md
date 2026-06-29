# MetroPT data quality report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/metropt_data_quality_report.md`

## 这份文档是什么

早期数据质量报告，说明 ODS 数据的行数、时间范围、采样和空值情况。

## 输入是什么

ODS Parquet readings。

## 输出是什么

数据质量 Markdown、JSON 摘要和样本趋势图。

## 怎么看

先看 Summary，再看 Daily Sample Quality 和 Null Counts，确认数据是否可继续分析。

## 关键术语

- `ODS`
- `data quality`
- `null count`
- `daily sample`
- `PASS`

## 证据边界

该报告是分析输入质量证明，不等于完整业务验收。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

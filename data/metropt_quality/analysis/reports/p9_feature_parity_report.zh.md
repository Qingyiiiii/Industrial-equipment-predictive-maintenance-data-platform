# P9 feature parity report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_feature_parity_report.md`

## 这份文档是什么

P9/P10 特征 parity 报告，比较 CSV-derived 与 warehouse-derived features。

## 输入是什么

CSV-derived P9 features、warehouse-derived P10 features。

## 输出是什么

parity summary、差异说明和可接受边界。

## 怎么看

重点看行数、时间范围、字段数量、标签分布和关键统计量。

## 关键术语

- `parity`
- `CSV-derived`
- `warehouse-derived`
- `feature table`
- `label distribution`

## 证据边界

差异必须可解释；不能把不一致静默写成完全等价。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

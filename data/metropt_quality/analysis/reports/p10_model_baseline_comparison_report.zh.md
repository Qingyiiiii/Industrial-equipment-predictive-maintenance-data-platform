# P10 model baseline comparison report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p10_model_baseline_comparison_report.md`

## 这份文档是什么

P10 模型对比报告，比较 CSV-derived 和 warehouse-derived features 上的 baseline 指标。

## 输入是什么

P9 CSV-derived features、P10 warehouse-derived features、时间切分标签。

## 输出是什么

模型指标对比、lead time 边界、RF/IF 可用性说明。

## 怎么看

先看 Metric Comparison，再看 Split And Leakage Checks；重点确认 warehouse-derived 结果是否可接受。

## 关键术语

- `P10`
- `warehouse-derived`
- `CSV-derived`
- `baseline`
- `lead time`
- `leakage`

## 证据边界

不要只看单个指标；必须保留时间切分和 leakage boundary。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

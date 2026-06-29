# P9 model baseline report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_model_baseline_report.md`

## 这份文档是什么

P9 baseline model 报告，说明时间切分后的模型指标、lead time 和局限性。

## 输入是什么

P9 minute features、weak labels、time split。

## 输出是什么

model metrics、prediction sample、feature weights、figures。

## 怎么看

先看 Dataset and Target、Time Split、Metrics，再看 Lead Time 和 Limitations。

## 关键术语

- `P9`
- `baseline`
- `Logistic Regression`
- `Random Forest`
- `lead time`
- `PR-AUC`

## 证据边界

baseline 不是生产预测模型；lead time 必须说明模型来源。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

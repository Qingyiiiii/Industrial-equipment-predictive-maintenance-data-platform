# MetroPT baseline model report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/metropt_baseline_model_report.md`

## 这份文档是什么

早期 baseline model 报告，说明基于 DWS window KPI 的故障窗口分类尝试和模型边界。

## 输入是什么

DWS window KPI、弱标签 failure_window、时间切分后的训练/验证数据。

## 输出是什么

baseline metrics、feature signals、confusion matrix figure。

## 怎么看

先看 Dataset 和 Metrics，再看 Top Feature Signals，最后看 Limitations。重点判断模型是否只是 baseline，而不是生产预测模型。

## 关键术语

- `baseline model`
- `DWS`
- `failure_window`
- `precision`
- `recall`
- `f1`
- `PR-AUC`

## 证据边界

弱标签来自故障窗口派生，不能当成人工逐行标注；baseline 只用于说明可建模性。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

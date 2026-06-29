# MetroPT multidimensional analysis report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/metropt_multidim_analysis_report.md`

## 这份文档是什么

多维 EDA 报告，解释运行状态、传感器波动、故障窗口对比和建模边界。

## 输入是什么

ODS/DWD/DWS 数据和 failure windows。

## 输出是什么

多维分析结论、图表和 JSON summary。

## 怎么看

先看 Executive Summary，再看 Operating State、Sensors Worth Tracking First 和 Failure Window Contrast。

## 关键术语

- `EDA`
- `operating_state`
- `sensor correlation`
- `failure window`
- `modeling boundary`

## 证据边界

EDA 用于理解现象，不代表模型已上线。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

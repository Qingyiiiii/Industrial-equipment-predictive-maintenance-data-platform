# P9 cluster validation checklist（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_master_validation_checklist.md`

## 这份文档是什么

P9 cluster 复验清单，说明如何检查 P9 成果、依赖、语法、特征、模型和实时设计。

## 输入是什么

P9 P9 artifacts、analysis reports、scripts。

## 输出是什么

复验命令、检查项、失败处理建议。

## 怎么看

按 checklist 执行，先检查依赖和 artifact presence，再跑质量检查。

## 关键术语

- `P9`
- `cluster validation`
- `checklist`
- `P9 evidence`
- `PASS`
- `WARN`

## 证据边界

这是复验流程，不是结果报告；结果看 p9_master_validation_result_20260606.md。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

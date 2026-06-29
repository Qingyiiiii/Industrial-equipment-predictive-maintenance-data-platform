# P9 QA checklist（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_qa_checklist.md`

## 这份文档是什么

P9 QA 清单，说明本地自验、文档边界和 cluster 复验要求。

## 输入是什么

P9 artifacts、P9 evidence bundles、reports。

## 输出是什么

QA status、检查项和待 cluster 验证边界。

## 怎么看

先看 Required context 和 QA checks，再看 cluster validation notes。

## 关键术语

- `P9`
- `QA`
- `checklist`
- `cluster validation`
- `boundary`

## 证据边界

旧 master pending 已由后续证据覆盖，当前看 overlay 表。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

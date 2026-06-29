# P13 BI field semantics（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p13_bi_field_semantics.md`

## 这份文档是什么

P13 字段语义文档，说明 BI 字段来源、口径和弱标签边界。

## 输入是什么

P9/P10/P11/P12 字段、Hive/Trino/Doris/analysis artifacts。

## 输出是什么

字段语义、来源等级、弱标签说明。

## 怎么看

先按字段组查看来源，再看 production boundary。

## 关键术语

- `field semantics`
- `weak label`
- `risk_score`
- `source level`
- `production boundary`

## 证据边界

弱标签不能包装成人工标注；实时 risk score 不能包装成生产模型概率。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

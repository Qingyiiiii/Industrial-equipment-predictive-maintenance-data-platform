# P12 query layer validation report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p12_query_layer_validation_report.md`

## 这份文档是什么

P12 查询层复验报告，说明 Trino/Doris 查询样例、Doris 装载映射和 Hive 口径一致性。

## 输入是什么

Hive canonical tables、Trino schema、Doris OLAP table、P9 dashboard SQL。

## 输出是什么

p12_query_results.tsv、p12_consistency.tsv、SQL logs。

## 怎么看

先看 summary 是否 fail=0，再看 query results 的 rc 和 consistency 表。

## 关键术语

- `P12`
- `Trino`
- `Doris`
- `Hive`
- `consistency`
- `query results`
- `rc=0`

## 证据边界

P12 是 P9 查询层正式证据；不要用旧 P5 结果替代。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

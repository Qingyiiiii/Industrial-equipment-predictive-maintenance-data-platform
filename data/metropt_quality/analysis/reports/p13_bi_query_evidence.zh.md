# P13 BI query evidence（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p13_bi_query_evidence.md`

## 这份文档是什么

P13 查询证据文档，绑定 SQL、样例输出和 P12/P11/P10/P9 证据。

## 输入是什么

BI SQL、sample outputs、P12 query results、P11 risk samples。

## 输出是什么

每个看板页对应的 SQL 和样例输出。

## 怎么看

按页面看 SQL，再看 sample output 是否能支撑图表。

## 关键术语

- `query evidence`
- `SQL`
- `sample output`
- `Hive`
- `Trino`
- `Doris`
- `P12`

## 证据边界

SQL 样例用于作品集说明，不能替代完整 P14 验收。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

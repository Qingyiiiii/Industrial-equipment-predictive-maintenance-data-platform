# P9 dashboard field dictionary（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_dashboard_field_dictionary.md`

## 这份文档是什么

P9 dashboard 字段字典，说明看板字段、SQL 样例和字段口径。

## 输入是什么

P9 analysis artifacts、Hive/DWS fields、dashboard SQL draft。

## 输出是什么

字段字典、查询样例、BI 字段说明。

## 怎么看

先看字段来源，再看 SQL 样例；Trino/Doris 部分以 P12 后续复验为准。

## 关键术语

- `P9`
- `dashboard`
- `field dictionary`
- `Hive`
- `Trino`
- `Doris`
- `SQL`

## 证据边界

P9 时点部分查询仍待 master/P12，当前已由 P12/P14 关闭。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

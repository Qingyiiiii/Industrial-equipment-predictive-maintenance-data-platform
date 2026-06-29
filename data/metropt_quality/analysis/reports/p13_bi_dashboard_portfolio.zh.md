# P13 BI dashboard portfolio（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p13_bi_dashboard_portfolio.md`

## 这份文档是什么

P13 BI 看板素材说明，固定 5 页展示结构和每页字段/SQL/证据绑定。

## 输入是什么

P9/P10/P11/P12 reports、SQL、sample outputs、figures。

## 输出是什么

BI 页面说明、字段来源、样例输出和边界脚注。

## 怎么看

按页面阅读：总体健康、传感器风险、故障窗口、模型表现、实时风险。

## 关键术语

- `P13`
- `BI dashboard`
- `portfolio`
- `Hive`
- `Trino`
- `Doris`
- `sample outputs`

## 证据边界

这是 BI 素材包，不是已经上线的 BI 产品。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

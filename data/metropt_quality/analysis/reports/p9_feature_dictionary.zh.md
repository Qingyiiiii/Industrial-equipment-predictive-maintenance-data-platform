# P9 feature dictionary（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_feature_dictionary.md`

## 这份文档是什么

P9 feature dictionary，说明 minute feature table 的字段组和设计规则。

## 输入是什么

P9 feature engineering 输出的 minute features。

## 输出是什么

feature dictionary 和 feature groups。

## 怎么看

先看 Generated Feature Table，再看 Feature Groups 和 Validation Boundary。

## 关键术语

- `P9`
- `feature dictionary`
- `minute features`
- `leakage`
- `time split`

## 证据边界

特征来自窗口统计，必须避免标签泄漏和未来信息泄漏。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

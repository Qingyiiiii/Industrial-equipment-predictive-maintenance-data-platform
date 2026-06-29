# P9 realtime risk design（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_realtime_risk_design.md`

## 这份文档是什么

P9 实时风险设计文档，定义风险分数字段契约和实时拓扑。

## 输入是什么

P9 labels、features、Kafka/Flink/Redis/Hive 设计。

## 输出是什么

risk contract、topology、field semantics。

## 怎么看

先看 scoring contract，再看 topology 和 production boundary。

## 关键术语

- `P9`
- `realtime risk`
- `risk_score`
- `Flink`
- `Redis`
- `Hive`
- `dry-run`

## 证据边界

P9 是设计和 dry-run 边界；P11 完成 Flink signal-proxy 接入。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

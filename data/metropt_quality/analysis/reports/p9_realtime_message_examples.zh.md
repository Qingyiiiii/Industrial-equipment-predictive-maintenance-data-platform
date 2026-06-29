# P9 realtime message examples（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p9_realtime_message_examples.md`

## 这份文档是什么

P9 实时消息样例文档，展示合法、增强和 DLQ 消息格式。

## 输入是什么

P9 realtime risk design、Kafka event schema。

## 输出是什么

JSON message examples、DLQ 示例和字段说明。

## 怎么看

先看 valid/enriched/invalid examples，再看字段解释。

## 关键术语

- `P9`
- `Kafka`
- `JSON`
- `DLQ`
- `risk_score`
- `message schema`

## 证据边界

样例不代表生产服务已上线；P11 才是 Flink 接入证据。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

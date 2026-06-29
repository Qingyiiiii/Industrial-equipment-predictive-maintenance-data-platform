# P11 realtime risk scoring validation report（中文版）

原英文文件：`data/metropt_quality/analysis/reports/p11_realtime_risk_scoring_validation_report.md`

## 这份文档是什么

P11 实时风险评分验收报告，说明 Flink signal-proxy scorer 已接入 Kafka/Flink/Redis/Hive 链路。

## 输入是什么

Kafka replay events、Flink risk scoring job、Redis、Hive realtime risk table。

## 输出是什么

P11 run_dir、Redis risk sample、Hive risk sample、DLQ/schema 验收。

## 怎么看

先看 validation summary，再看 Redis/Hive 样例和 DLQ 结果。注意尾部已经说明 Trino/Doris pending 由 P12/P14 关闭。

## 关键术语

- `P11`
- `Flink`
- `risk_score`
- `risk_level`
- `Redis`
- `Hive`
- `DLQ`
- `model_version`

## 证据边界

当前 model_version 是 flink_signal_proxy_not_production_model，不能写成生产 ML 模型服务。

## 复现提示

如果要复现这份报告，不要只复制报告文件。应回到对应脚本或验收 run_dir，重新运行生成步骤，并保留新的 `summary.tsv`、日志和输出文件。英文原文是证据原件，本文用于中文读者快速理解。

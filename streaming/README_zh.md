# streaming 实时链路说明

语言 / Language: [中文](README_zh.md) | [English](README_en.md)

`streaming/` 负责把 MetroPT-3 静态 CSV 模拟成设备实时流，并通过 Kafka、Flink、Hive、Redis 形成实时 KPI 和风险评分闭环。

当前实时链路是验证型 demo，不是常驻生产采集服务。Flink 作业可以短时运行后退出，不能仅凭“当前没有长作业”判断失败。

## 数据流

```text
MetroPT3_AirCompressor.csv
  -> metropt_replay_to_kafka.py
  -> Kafka topic: metropt.ods.compressor.reading.v1
  -> 01_flink_metropt_kafka_to_hive.py
  -> Hive realtime tables + Redis KPI
  -> 02_flink_metropt_realtime_risk_score.py
  -> Hive risk table + Redis latest risk
```

## 文件说明

| 文件 | 作用 | 输入 | 输出 |
| --- | --- | --- | --- |
| `metropt_replay_to_kafka.py` | 将 CSV 标准化为 JSON event 并发送到 Kafka；支持 dry-run | CSV、`metropt_quality.cluster.yaml` | Kafka events 或 dry-run JSON |
| `01_flink_metropt_kafka_to_hive.py` | Flink SQL job，消费 Kafka，写 Hive realtime ODS/KPI 和 Redis KPI | Kafka topic | `ods_metropt_realtime_readings`、`dws_metropt_realtime_kpi_1min`、Redis KPI key |
| `02_flink_metropt_realtime_risk_score.py` | Flink risk scoring job，生成 `risk_score`、`risk_level`、`risk_reason` | Kafka topic | `dws_metropt_realtime_risk_events`、Redis risk key |
| `metropt_realtime_risk_score_plan.py` | P9 dry-run 风险契约和消息样例，不直接代表生产模型 | JSONL 或内置样例 | valid/invalid/enriched risk messages |

## 快速使用

dry-run 检查 JSON：

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --dry-run \
  --print-sample 3 \
  --max-events 3
```

发送小批量事件：

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --rate 500 \
  --batch-size 500 \
  --max-events 10000
```

风险契约 dry-run：

```bash
python streaming/metropt_realtime_risk_score_plan.py --emit-examples all
```

推荐使用验收脚本跑完整实时 demo：

```bash
bin/p6_realtime_demo_mode.sh --start --duration-minutes 0 --max-events 1000 --rate 500 --wait-seconds 60
```

单独验收 P11 风险评分：

```bash
bin/p11_realtime_risk_acceptance.sh --max-events 1000 --rate 500 --startup-mode earliest-offset
```

## 关键输入输出

| 对象 | 名称 |
| --- | --- |
| Kafka 主 topic | `metropt.ods.compressor.reading.v1` |
| Kafka DLQ topic | `metropt.ods.compressor.reading.dlq.v1` |
| Redis KPI pattern | `metropt:kpi:1m:*` |
| Redis risk pattern | `metropt_quality:risk:latest:*` |
| Hive realtime KPI | `metropt_quality.dws_metropt_realtime_kpi_1min` |
| Hive risk table | `metropt_quality.dws_metropt_realtime_risk_events` |

## 怎么看结果

1. 看验收 run_dir 的 `summary.tsv`，确认 `fail=0`。
2. 看 Redis scan 日志是否出现 KPI 或 risk key。
3. 看 Hive 查询日志是否 `rc=0`。
4. 看 DLQ 测试是否能识别 invalid message。
5. 看 `model_version`，当前是 `flink_signal_proxy_not_production_model`，不能写成生产 ML 模型服务。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| Kafka 连接失败 | 先跑 `bin/start_realtime_mode.sh --check-only`，再检查 broker、topic、JDK17 |
| Redis 没有 key | 看 Flink submit log、等待策略、Redis pattern 是否正确 |
| Hive realtime 表为空 | 检查 Flink job 是否消费到 Kafka，确认 startup mode 和 group id |
| DLQ 没结果 | 确认是否使用了 `--inject-dlq-test` |
| 当前 Flink job 不在运行 | 如果 demo summary 是 PASS，短时作业退出是正常状态 |

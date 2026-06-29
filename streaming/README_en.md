# streaming Real-Time Pipeline

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

`streaming/` turns the static MetroPT-3 CSV into a simulated device event stream. Kafka, Flink, Hive, and Redis are used to build a real-time KPI and risk-scoring validation loop.

The current real-time pipeline is a validation demo, not a resident production collector. Flink jobs may finish and exit after a short run, so the absence of a long-running job is not enough to mark the pipeline as failed.

## Data Flow

```text
MetroPT3_AirCompressor.csv
  -> metropt_replay_to_kafka.py
  -> Kafka topic: metropt.ods.compressor.reading.v1
  -> 01_flink_metropt_kafka_to_hive.py
  -> Hive realtime tables + Redis KPI
  -> 02_flink_metropt_realtime_risk_score.py
  -> Hive risk table + Redis latest risk
```

## Files

| File | Purpose | Input | Output |
| --- | --- | --- | --- |
| `metropt_replay_to_kafka.py` | Standardizes CSV rows into JSON events and sends them to Kafka; supports dry-run | CSV, `metropt_quality.cluster.yaml` | Kafka events or dry-run JSON |
| `01_flink_metropt_kafka_to_hive.py` | Flink SQL job consuming Kafka and writing Hive realtime ODS/KPI plus Redis KPI | Kafka topic | `ods_metropt_realtime_readings`, `dws_metropt_realtime_kpi_1min`, Redis KPI key |
| `02_flink_metropt_realtime_risk_score.py` | Flink risk scoring job producing `risk_score`, `risk_level`, and `risk_reason` | Kafka topic | `dws_metropt_realtime_risk_events`, Redis risk key |
| `metropt_realtime_risk_score_plan.py` | P9 dry-run risk contract and message examples; not a production model | JSONL or built-in examples | Valid, invalid, and enriched risk messages |

## Quick Use

Dry-run JSON inspection:

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --dry-run \
  --print-sample 3 \
  --max-events 3
```

Send a small event batch:

```bash
python streaming/metropt_replay_to_kafka.py \
  --config /home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml \
  --rate 500 \
  --batch-size 500 \
  --max-events 10000
```

Risk contract dry-run:

```bash
python streaming/metropt_realtime_risk_score_plan.py --emit-examples all
```

Recommended full real-time demo validation:

```bash
bin/p6_realtime_demo_mode.sh --start --duration-minutes 0 --max-events 1000 --rate 500 --wait-seconds 60
```

Standalone P11 risk scoring validation:

```bash
bin/p11_realtime_risk_acceptance.sh --max-events 1000 --rate 500 --startup-mode earliest-offset
```

## Key Inputs And Outputs

| Object | Name |
| --- | --- |
| Kafka main topic | `metropt.ods.compressor.reading.v1` |
| Kafka DLQ topic | `metropt.ods.compressor.reading.dlq.v1` |
| Redis KPI pattern | `metropt:kpi:1m:*` |
| Redis risk pattern | `metropt_quality:risk:latest:*` |
| Hive realtime KPI | `metropt_quality.dws_metropt_realtime_kpi_1min` |
| Hive risk table | `metropt_quality.dws_metropt_realtime_risk_events` |

## How To Read Results

1. Check the validation run directory `summary.tsv` and confirm `fail=0`.
2. Check Redis scan logs for KPI or risk keys.
3. Check Hive query logs for `rc=0`.
4. Confirm DLQ tests can identify invalid messages.
5. Check `model_version`: current value is `flink_signal_proxy_not_production_model`, which must not be described as a production ML model service.

## Common Issues

| Symptom | Action |
| --- | --- |
| Kafka connection failure | Run `bin/start_realtime_mode.sh --check-only`, then check brokers, topics, and JDK17 |
| Redis has no key | Check Flink submit logs, wait strategy, and Redis pattern |
| Hive realtime table is empty | Confirm the Flink job consumed Kafka and check startup mode plus group id |
| No DLQ result | Confirm `--inject-dlq-test` was used |
| No current running Flink job | If the demo summary is PASS, a short-lived job exit is normal |


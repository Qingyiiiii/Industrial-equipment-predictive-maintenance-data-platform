# P11 Realtime Risk Scoring Validation Report

## Decision

Validation owner: 验证负责人

Validation date: 2026-06-06 cluster time / 2026-06-07 local thread date

Result: **PASS**

P11 has connected the P9 realtime-risk contract into the real Kafka/Flink/Redis/Hive path. The online scorer is a Flink signal-proxy scorer that preserves the P9 dry-run contract formula and emits risk fields online. It is not a claim that a production ML model service has replaced the proxy scorer.

## Implemented Artifacts

| Artifact | Purpose |
| --- | --- |
| `streaming/02_flink_metropt_realtime_risk_score.py` | Kafka raw event source, schema validation, signal-proxy risk scoring, Hive risk sink, Redis latest-risk sink, DLQ sink |
| `bin/p11_realtime_risk_acceptance.sh` | P11 acceptance script for Kafka/Flink/Redis/Hive/DLQ evidence |
| `bin/p6_realtime_demo_mode.sh` | P6 demo now runs P11 risk acceptance by default and captures Redis/Hive risk samples |
| `config/metropt_quality.cluster.yaml` | Adds `hive.dws_realtime_risk_table` and `realtime.redis_risk_key_prefix` |
| `config/metropt_quality.local.yaml` | Mirrors the P11 risk table and Redis risk keys for local config consistency |

## Runtime Interfaces

| Interface | Value |
| --- | --- |
| Kafka input topic | `metropt.ods.compressor.reading.v1` |
| Kafka DLQ topic | `metropt.ods.compressor.reading.dlq.v1` |
| Hive risk table | `metropt_quality.dws_metropt_realtime_risk_events` |
| Redis latest-risk key | `metropt_quality:risk:latest:compressor_1` |
| Risk model version | `p11_flink_signal_proxy_20260607` |
| Risk score source | `flink_signal_proxy_not_production_model` |

Risk output fields verified online:

```text
risk_score
risk_level
risk_reason
model_version
risk_model_version
risk_score_source
model_feature_set_version
```

## Validation Runs

| Run | Purpose | Result |
| --- | --- | --- |
| `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/start_realtime_mode_20260606_224600` | Start Kafka/Flink/Redis/Hive realtime mode | `pass=4 warn=0 skip=0 fail=0`, `return_code=0` |
| `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p11_realtime_risk_20260606_224727` | Full P11 run, 10000 normal replay events | `pass=14 warn=1 skip=0 fail=0`, `return_code=0`; warning only because DLQ injection was intentionally skipped |
| `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p11_realtime_risk_20260606_225533` | P11 run with DLQ marker injection | `pass=16 warn=0 skip=0 fail=0`, `return_code=0` |
| `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p6_realtime_demo_20260606_230140` | P6 demo with default P11 risk acceptance enabled | `SUMMARY pass=10 warn=0 skip=0 fail=0`, `return_code=0` |

## Key Evidence

Kafka input:

- `p11_realtime_risk_20260606_224727/replay_send.log`: `sent=10000`, `failed=0`.
- `p11_realtime_risk_20260606_225533/replay_send.log`: `sent=1000`, `failed=0`.

Flink processing:

- `p11_realtime_risk_20260606_225533/flink_risk_submit.log` contains:

```text
MetroPT P11 Flink 风险作业已提交: topic=metropt.ods.compressor.reading.v1, risk_table=metropt_quality.dws_metropt_realtime_risk_events, redis=metropt_quality:risk:latest:compressor_1
```

Hive risk query:

- `p11_realtime_risk_20260606_225533/realtime_risk_hive_check.log` returned `rc=0`.
- `SHOW TABLES LIKE '*risk*'` returned `dws_metropt_realtime_risk_events`.
- `SHOW PARTITIONS` returned `dt=2020-02-01` and `dt=2020-02-02`.
- Sample output included:

```text
metropt-30    2020-02-01 00:00:29.0    stopped    0.24906    low    pressure_balance_shift    p11_flink_signal_proxy_20260607
```

Redis latest-risk sample:

- `p11_realtime_risk_20260606_225533/redis_risk_check.log` returned `rc=0`.
- Key: `metropt_quality:risk:latest:compressor_1`.
- Sample fields:

```text
risk_score=0.251310
risk_level=low
risk_reason=pressure_balance_shift
model_version=p11_flink_signal_proxy_20260607
```

DLQ/schema validation:

- Bad event marker: `p11_dlq_20260606_225533`.
- `p11_realtime_risk_20260606_225533/dlq_inject_check.log` observed:

```text
{"reason":"missing_event_id_or_parse_error","event_id":null,"raw_index":null,"payload":"source=p11_dlq_20260606_225533,state=null,failure_type=null","write_time":"2026-06-06 22:56:35"}
```

P6 demo risk proof:

- `p6_realtime_demo_20260606_230140/demo_status.tsv` contains `with_risk=1`, `p11_risk_acceptance_log`, `redis_risk_key_sample_count=1`, and `hive_realtime_risk_sample=PASS`.
- `p6_realtime_demo_20260606_230140/redis_risk_sample.log` contains `risk_score`, `risk_level`, `risk_reason`, and `model_version`.
- `p6_realtime_demo_20260606_230140/hive_realtime_risk_sample.log` queries `dws_metropt_realtime_risk_events` and returns risk fields.

## Boundaries

- The P11 scorer is an online signal-proxy scorer derived from the P9 contract; it is not a production ML model service.
- Repeated validation runs use `earliest-offset`, so `dws_metropt_realtime_risk_events` is append-only validation evidence and may contain duplicate event IDs across reruns.
- The final DLQ hard-evidence run used `max_events=1000` to keep the validation bounded. The earlier full normal run used `max_events=10000`.
- At the time of P11 validation, Trino/Doris P9 query samples were not covered by this report. That pending item was later closed by P12 query-layer validation and P14 cluster validation; keep this P11 report as realtime-risk evidence only.

## Next Entry

For query-layer evidence, read `p12_query_layer_validation_report.md` and the latest P14 cluster validation report. Do not use this P11 report alone to claim Trino/Doris query-layer validation.

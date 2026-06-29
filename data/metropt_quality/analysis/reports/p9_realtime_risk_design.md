# P9 Realtime Risk Score Design

## Scope

This document covers the worker realtime-data-engineering scope for P9. It reviews the current Kafka replay and Flink SQL realtime path, then defines a risk-score message contract that can be validated locally without connecting to Kafka, Flink, Redis, Hive, or Doris.

The local validation pass did not run cluster services in this round. Kafka, Flink, Redis, Hive, Iceberg, Trino, Doris, and HDFS conclusions in this document are 待 cluster 验证.

## Inputs Reviewed

| Input | Review result |
| --- | --- |
| `streaming/metropt_replay_to_kafka.py` | Replay producer normalizes raw CSV fields and emits JSON events. |
| `streaming/01_flink_metropt_kafka_to_hive.py` | Flink SQL job reads Kafka JSON, splits valid/DLQ events, writes Hive realtime tables, and writes Redis KPI side effects. |
| `config/metropt_quality.local.yaml` | Local config keeps realtime topic, DLQ topic, Redis URL, Redis key prefix, replay rate, and batch size. |
| `config/metropt_quality.cluster.yaml` | Cluster config points replay CSV to the VM path and uses the same realtime topic/key names. |
| `data/metropt_quality/analysis/models/p9_model_metrics.json` | Provides P9 baseline model metadata, threshold, feature list, metrics, and limitations. |
| `data/metropt_quality/analysis/models/p9_model_prediction_sample.tsv` | Provides sample `numpy_logistic_score` values for the P9 baseline target `pre_failure_24h`. |
| `data/metropt_quality/validation_runs/p6_realtime_demo_20260605_230423/demo_summary.md` | Historical master evidence says P6 bounded realtime demo sent 10000 records with 0 replay failures. |
| `data/metropt_quality/delivery_packages/p8_delivery_package_20260606_011332/realtime_demo_steps.md` | Historical P8 realtime demo instructions and evidence pointer. |

## Current Realtime Chain Review

### Replay Message Fields

`streaming/metropt_replay_to_kafka.py` emits the required realtime event fields:

| Field group | Status | Evidence |
| --- | --- | --- |
| Event identity | present | `event_id`, `raw_index` |
| Time fields | present | `event_time`, `ingest_time` |
| State fields | present | `operating_state`, `is_failure_window`, `failure_type` |
| Analog sensors | present | `tp2`, `tp3`, `h1`, `dv_pressure`, `reservoirs`, `oil_temperature`, `motor_current` |
| Digital sensors | present | `comp`, `dv_electric`, `towers`, `mpg`, `lps`, `pressure_switch`, `oil_level`, `caudal_impulses` |

The replay script also standardizes raw `DV_eletric` / `DV_electric` to `dv_electric`.

### Topic, DLQ, and Redis Naming

Current config values:

| Item | Current value | Local review |
| --- | --- | --- |
| Kafka source topic | `metropt.ods.compressor.reading.v1` | Inherits P6/P8 realtime demo naming. Worker does not rename it in this round. |
| Kafka DLQ topic | `metropt.ods.compressor.reading.dlq.v1` | Inherits P6/P8 realtime demo naming. Worker does not rename it in this round. |
| Flink group id | `metropt_quality_flink_v1` | Uses the `metropt_quality` project domain. |
| Hive database | `metropt_quality` | Uses the `metropt_quality` project domain. |
| Redis KPI prefix | `metropt:kpi:1m` | Inherits P6/P8 realtime demo naming. Worker does not rename it in this round. |

P9 risk-score outputs should use an explicit project-domain prefix when master approves new realtime artifacts:

| Proposed P9 artifact | Proposed name | Status |
| --- | --- | --- |
| Risk score Kafka topic | `metropt_quality.risk.compressor.score.v1` | proposal only, 待 cluster 验证 |
| Risk score DLQ topic | `metropt_quality.risk.compressor.score.dlq.v1` | proposal only, 待 cluster 验证 |
| Latest risk Redis key prefix | `metropt_quality:risk:1m` | proposal only, 待 cluster 验证 |

This proposal avoids changing existing P6/P8 topics and Redis keys. If master prefers the existing `metropt.*` topic family for compatibility, keep P6/P8 names and document the business domain through database/group/pipeline names instead.

### DLQ and Failure Handling

Current handling:

- Replay producer logs Kafka send failures as `[replay] send_failed ...` and increments `failed`.
- Flink job defines a `kafka_metropt_dlq` table and sends parsed records with missing required fields to DLQ.
- Flink job prints the DLQ v1 scope: parsed events with missing identity, time, state, or sensor fields are covered; raw malformed JSON payload capture is a future optimization.
- Redis UDF logs `[redis_udf] write_failed ...` and returns `0` to the blackhole sink.

P9 risk integration should keep these rules and add:

- A risk-score DLQ reason for missing model metadata or invalid `risk_score`.
- A Redis write success/failure counter in the realtime validation evidence.
- A master-side decision on whether Redis write failure should fail the job or be reported as a degraded side effect.

## Risk Score Contract

### Raw Event Contract

The raw replay event remains the source of truth. The online scorer must not use P9 label fields as features. In particular, `is_failure_window`, `failure_type`, `pre_failure_1h`, `pre_failure_6h`, `pre_failure_24h`, `post_maintenance`, `normal_candidate`, and `rul_seconds` are not online model input features.

### Enriched Event Contract

An enriched risk-score event should add these fields:

| Field | Meaning | Required for P9 risk output |
| --- | --- | --- |
| `risk_score` | Bounded model score in `[0, 1]`. | yes |
| `risk_level` | `low`, `medium`, or `high` derived from the active threshold. | yes |
| `risk_score_source` | Source of the score, such as `p9_model_prediction_sample.tsv`, `numpy_logistic_regression`, or a later model artifact. | yes |
| `risk_model_name` | Model family or scorer id. | yes |
| `risk_model_version` | Version string tied to a model artifact and feature set. | yes |
| `risk_threshold` | Threshold used for `risk_level` and alarm candidate generation. | yes |
| `feature_window_minutes` | Right-aligned windows used by the scorer, currently `[1, 5, 15, 60]`. | yes |
| `feature_window_end` | Event time or minute bucket that closes the feature window. | yes |
| `model_feature_set_version` | Feature dictionary version, currently `p9_window_features_v1`. | yes |
| `risk_reason` | Short signal explanations, for example pressure balance, oil temperature, or motor current. | optional but recommended |
| `scoring_time` | UTC scoring time. | yes |

### Batch Baseline Link

Current P9 baseline assets:

| Asset | How realtime should use it |
| --- | --- |
| `p9_model_metrics.json` | Read baseline model name, threshold, feature list, validation/test metrics, and limitations. |
| `p9_logistic_feature_weights.tsv` | Candidate explanation source for top feature signals. |
| `p9_model_prediction_sample.tsv` | Contract sample for `numpy_logistic_score`; not a complete online scorer. |
| `p9_window_features_1min.parquet` | Offline feature table showing the required 1/5/15/60 minute right-aligned feature shape. |

The current local dry-run script can pass through `numpy_logistic_score` as `risk_score` when a sample record already contains it. For raw replay events that do not contain model output, it can attach a clearly marked `dry_run_signal_proxy_not_production` score only to validate the downstream field contract.

## Proposed Realtime Topology

1. Existing replay producer writes raw readings to `metropt.ods.compressor.reading.v1`.
2. Existing Flink job validates the raw JSON shape, writes invalid parsed events to DLQ, writes valid events to Hive realtime ODS, and writes 1-minute KPI side effects to Redis.
3. A future P9 risk stage computes right-aligned 1/5/15/60 minute features from valid events. This stage must use only current and past events.
4. The risk scorer loads an approved P9 model artifact and threshold. The first production candidate should come from a cluster-rerun baseline artifact, not from the local dry-run proxy.
5. The risk scorer emits enriched events to a new risk topic and writes latest risk state to Redis using a P9 risk prefix.
6. BI can read either the Kafka risk topic, a Hive risk sink table, or Redis latest-risk keys after master validates the short demo.

## Candidate Hive/Redis Shape

Candidate risk sink table, pending master approval:

```sql
CREATE TABLE IF NOT EXISTS metropt_quality.dws_metropt_realtime_risk_1min (
  minute_bucket STRING,
  event_id STRING,
  raw_index BIGINT,
  operating_state STRING,
  risk_score DOUBLE,
  risk_level STRING,
  risk_score_source STRING,
  risk_model_name STRING,
  risk_model_version STRING,
  risk_threshold DOUBLE,
  risk_reason STRING
) PARTITIONED BY (dt STRING)
STORED AS PARQUET;
```

Candidate Redis key:

```text
metropt_quality:risk:1m:<equipment_id>:<minute_bucket>
```

Candidate Redis fields:

```text
risk_score
risk_level
risk_model_name
risk_model_version
risk_threshold
event_id
raw_index
operating_state
scoring_time
```

## Worker Dry-Run Entry

Local contract helper:

```powershell
py streaming\metropt_realtime_risk_score_plan.py --emit-examples all
py streaming\metropt_realtime_risk_score_plan.py --emit-examples valid --enrich
```

This helper is only for message-shape validation and documentation. It does not prove Kafka, Flink, Redis, Hive, or online scoring is running.

## Cluster Validation Needed

Cluster validation should decide and verify:

- Whether to keep existing P6/P8 topic/key naming or introduce explicit `metropt_quality.*` risk topics.
- Whether risk scoring is implemented as a Flink UDF, a separate stream processor, or a sidecar model service.
- Whether Redis write failure should fail the Flink job or be recorded as degraded side-effect evidence.
- Whether the risk sink table enters Hive/Doris/Trino query paths.
- Whether a cluster-rerun `random_forest` or `isolation_forest` baseline replaces the local `numpy_logistic_regression` baseline for risk output.

Suggested master-side smoke test after implementation:

```bash
cd /home/common/tmp/pycharm_Design
python streaming/metropt_realtime_risk_score_plan.py --emit-examples valid --enrich
bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500
```

The second command remains a cluster command and is 待 cluster 验证.

## Limitations

- Current P9 risk design is a contract and implementation plan, not an online scoring deployment.
- The local validation pass did not modify the existing P6/P8 realtime Flink job or Kafka/Redis config.
- Local dry-run proxy scores are not model metrics and must not be reported as production predictions.
- P9 labels are weak labels derived from configured failure windows; they must not be used as online features.

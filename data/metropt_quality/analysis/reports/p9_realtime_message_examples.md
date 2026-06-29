# P9 Realtime Message Examples

## Scope

This document gives local JSON examples for the P9 realtime risk-score contract. Examples are for local schema review only. The local validation pass did not send these payloads to Kafka and did not run Flink or Redis.

## Required Raw Replay Fields

Every valid raw replay event must contain:

```text
event_id
raw_index
event_time
ingest_time
operating_state
is_failure_window
failure_type
tp2
tp3
h1
dv_pressure
reservoirs
oil_temperature
motor_current
comp
dv_electric
towers
mpg
lps
pressure_switch
oil_level
caudal_impulses
```

`operating_state` should be one of `loaded`, `unloaded`, or `stopped`. `is_failure_window` should be `0` or `1`. Sensor fields should be numeric.

## Valid Raw JSON Examples

These examples match the current replay event shape.

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":0.0,"dv_pressure":-0.0229,"event_id":"metropt-0","event_time":"2020-02-01 00:00:00","failure_type":"normal","h1":9.3111,"ingest_time":"2026-06-06 12:47:43","is_failure_window":0,"lps":0.0,"motor_current":0.0404,"mpg":1.0,"oil_level":1.0,"oil_temperature":53.5214,"operating_state":"stopped","pressure_switch":1.0,"raw_index":0,"reservoirs":9.328,"source":"metropt_csv_replay","towers":1.0,"tp2":-0.0123,"tp3":9.3274}
```

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":0.0,"dv_pressure":-0.024,"event_id":"metropt-7200","event_time":"2020-02-01 02:00:00","failure_type":"normal","h1":8.906,"ingest_time":"2026-06-06 12:47:44","is_failure_window":0,"lps":0.0,"motor_current":3.95,"mpg":0.0,"oil_level":1.0,"oil_temperature":55.12,"operating_state":"unloaded","pressure_switch":1.0,"raw_index":7200,"reservoirs":8.934,"source":"metropt_csv_replay","towers":1.0,"tp2":1.842,"tp3":8.915}
```

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":0.0,"dv_pressure":-0.022,"event_id":"metropt-14400","event_time":"2020-02-01 04:00:00","failure_type":"normal","h1":8.731,"ingest_time":"2026-06-06 12:47:45","is_failure_window":0,"lps":0.0,"motor_current":7.32,"mpg":1.0,"oil_level":1.0,"oil_temperature":58.45,"operating_state":"loaded","pressure_switch":1.0,"raw_index":14400,"reservoirs":8.768,"source":"metropt_csv_replay","towers":1.0,"tp2":6.112,"tp3":8.74}
```

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":1.0,"dv_pressure":-0.026,"event_id":"metropt-6696000","event_time":"2020-04-18 00:00:00","failure_type":"air_leak_high_stress","h1":7.196,"ingest_time":"2026-06-06 12:47:46","is_failure_window":1,"lps":0.0,"motor_current":8.1,"mpg":1.0,"oil_level":1.0,"oil_temperature":64.3,"operating_state":"loaded","pressure_switch":1.0,"raw_index":6696000,"reservoirs":7.849,"source":"metropt_csv_replay","towers":1.0,"tp2":5.781,"tp3":7.214}
```

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":1.0,"dv_pressure":-0.029,"event_id":"metropt-14772000","event_time":"2020-07-15 14:30:00","failure_type":"air_leak_high_stress","h1":6.95,"ingest_time":"2026-06-06 12:47:47","is_failure_window":1,"lps":0.0,"motor_current":8.75,"mpg":1.0,"oil_level":1.0,"oil_temperature":68.2,"operating_state":"loaded","pressure_switch":1.0,"raw_index":14772000,"reservoirs":7.91,"source":"metropt_csv_replay","towers":1.0,"tp2":4.96,"tp3":6.98}
```

## Enriched Risk JSON Examples

These examples show the proposed P9 risk output shape. `risk_score_source` is deliberately marked as dry-run proxy when no true model score is present.

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":0.0,"dv_pressure":-0.0229,"event_id":"metropt-0","event_time":"2020-02-01 00:00:00","failure_type":"normal","feature_window_end":"2020-02-01 00:00:00","feature_window_minutes":[1,5,15,60],"h1":9.3111,"ingest_time":"2026-06-06 12:47:43","is_failure_window":0,"lps":0.0,"model_feature_set_version":"p9_window_features_v1","motor_current":0.0404,"mpg":1.0,"oil_level":1.0,"oil_temperature":53.5214,"operating_state":"stopped","pressure_switch":1.0,"raw_index":0,"reservoirs":9.328,"risk_level":"low","risk_model_name":"p9_realtime_contract","risk_model_version":"p9_worker_dry_run_20260606","risk_reason":["baseline_signal_level"],"risk_score":0.249165,"risk_score_source":"dry_run_signal_proxy_not_production","risk_threshold":0.5636003790254064,"scoring_time":"<runtime_utc>","source":"metropt_csv_replay","towers":1.0,"tp2":-0.0123,"tp3":9.3274}
```

```json
{"caudal_impulses":1.0,"comp":1.0,"dv_electric":1.0,"dv_pressure":-0.026,"event_id":"metropt-6696000","event_time":"2020-04-18 00:00:00","failure_type":"air_leak_high_stress","feature_window_end":"2020-04-18 00:00:00","feature_window_minutes":[1,5,15,60],"h1":7.196,"ingest_time":"2026-06-06 12:47:46","is_failure_window":1,"lps":0.0,"model_feature_set_version":"p9_window_features_v1","motor_current":8.1,"mpg":1.0,"oil_level":1.0,"oil_temperature":64.3,"operating_state":"loaded","pressure_switch":1.0,"raw_index":6696000,"reservoirs":7.849,"risk_level":"medium","risk_model_name":"p9_realtime_contract","risk_model_version":"p9_worker_dry_run_20260606","risk_reason":["pressure_balance_shift","oil_temperature_elevated","motor_current_elevated"],"risk_score":0.501897,"risk_score_source":"dry_run_signal_proxy_not_production","risk_threshold":0.5636003790254064,"scoring_time":"<runtime_utc>","source":"metropt_csv_replay","towers":1.0,"tp2":5.781,"tp3":7.214}
```

```json
{"event_id":"metropt-pred-sample-2020-07-01-000000","event_time":"2020-07-01 00:00:00","feature_window_end":"2020-07-01 00:00:00","feature_window_minutes":[1,5,15,60],"model_feature_set_version":"p9_window_features_v1","numpy_logistic_score":0.5906467758650229,"operating_state":"loaded","raw_index":-1,"risk_level":"high","risk_model_name":"p9_realtime_contract","risk_model_version":"p9_worker_dry_run_20260606","risk_reason":["passed_through_numpy_logistic_score"],"risk_score":0.590647,"risk_score_source":"p9_model_prediction_sample.tsv","risk_threshold":0.5636003790254064,"scoring_time":"<runtime_utc>"}
```

The third example is a contract bridge from `p9_model_prediction_sample.tsv`. It is not a raw replay event because it omits the full sensor payload.

## Invalid or DLQ Examples

| Example | Payload | Expected reason |
| --- | --- | --- |
| Missing event time | `{"event_id":"metropt-0","raw_index":0,"ingest_time":"2026-06-06 12:47:43","operating_state":"stopped","is_failure_window":0,"failure_type":"normal","tp2":-0.0123,"tp3":9.3274,"h1":9.3111,"dv_pressure":-0.0229,"reservoirs":9.328,"oil_temperature":53.5214,"motor_current":0.0404,"comp":1.0,"dv_electric":0.0,"towers":1.0,"mpg":1.0,"lps":0.0,"pressure_switch":1.0,"oil_level":1.0,"caudal_impulses":1.0}` | `missing_required_field:event_time` |
| Missing standardized sensor | `{"event_id":"metropt-7200","raw_index":7200,"event_time":"2020-02-01 02:00:00","ingest_time":"2026-06-06 12:47:44","operating_state":"unloaded","is_failure_window":0,"failure_type":"normal","tp2":1.842,"tp3":8.915,"h1":8.906,"dv_pressure":-0.024,"reservoirs":8.934,"oil_temperature":55.12,"motor_current":3.95,"comp":1.0,"towers":1.0,"mpg":0.0,"lps":0.0,"pressure_switch":1.0,"oil_level":1.0,"caudal_impulses":1.0}` | `missing_required_field:dv_electric` |
| Non-numeric sensor | `{"event_id":"metropt-14400","raw_index":14400,"event_time":"2020-02-01 04:00:00","ingest_time":"2026-06-06 12:47:45","operating_state":"loaded","is_failure_window":0,"failure_type":"normal","tp2":6.112,"tp3":8.74,"h1":8.731,"dv_pressure":-0.022,"reservoirs":8.768,"oil_temperature":58.45,"motor_current":"not-a-number","comp":1.0,"dv_electric":0.0,"towers":1.0,"mpg":1.0,"lps":0.0,"pressure_switch":1.0,"oil_level":1.0,"caudal_impulses":1.0}` | `non_numeric_sensor:motor_current` |
| Malformed JSON | `{"event_id": "metropt-bad-json", "raw_index": 1, ` | `invalid_json` |

Current Flink DLQ v1 handles parsed JSON with missing fields. Raw malformed JSON capture is listed as a future optimization in the realtime design and requires master implementation/verification.

## Local Dry-Run Commands

```powershell
py streaming\metropt_realtime_risk_score_plan.py --emit-examples valid
py streaming\metropt_realtime_risk_score_plan.py --emit-examples valid --enrich
py streaming\metropt_realtime_risk_score_plan.py --emit-examples invalid
```

These commands only print local examples. They do not prove realtime chain execution.

## Boundary

- `risk_score` examples are contract examples unless they explicitly pass through a score from `p9_model_prediction_sample.tsv`.
- `is_failure_window` and `failure_type` are replay/history context fields, not online model features.
- Any claim that Kafka, Flink, Redis, Hive, Trino, Doris, or P9 online scoring has passed must come from cluster validation evidence.

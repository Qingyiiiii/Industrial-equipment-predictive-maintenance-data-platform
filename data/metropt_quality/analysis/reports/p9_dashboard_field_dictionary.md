# P9 Dashboard Field Dictionary

## Scope

This dictionary prepares BI fields, query samples, and dashboard semantics for P9. It uses current project table/view names and P9 analysis artifacts. The local validation pass did not run Hive, Trino, Doris, or cluster queries in its original P9 round.

Master validation update on 2026-06-07: P12 executed the Hive, Trino / Iceberg, and Doris query samples from this dictionary on the cluster. Evidence entry: `data/metropt_quality/analysis/reports/p12_query_layer_validation_report.md`; run_dir: `/home/common/tmp/pycharm_Design/data/metropt_quality/validation_runs/p12_query_layer_validation_20260607_035423`; summary: `pass=30 warn=0 skip=0 fail=0`.

## Field Dictionary

| Field | Meaning | Source layer or artifact | Used by model | BI usage | Validation status |
| --- | --- | --- | --- | --- | --- |
| `event_time` | Original event timestamp after standardization. | ODS / P9 CSV-derived features | no | Timeline, filters, fault markers | 待 cluster 验证 for ODS parity |
| `event_minute` | Minute-grain timestamp for feature rows. | DWS window KPI / P9 feature table | yes, as split key only | Minute trend and risk timeline | Checked locally |
| `minute_bucket` | BI-friendly minute bucket string. | `vw_pbi_metropt_window_kpi`, realtime KPI | no | BI axis and realtime panels | 待 cluster 验证 |
| `dt` | Date partition. | ODS/DWD/DWS/Hive views | no | Date filter and partition check | 待 cluster 验证 |
| `operating_state` | Derived compressor state: `loaded`, `unloaded`, or `stopped`. | DWS / P9 features | yes | State distribution and comparison | Checked locally |
| `sample_count` | Count of records in a minute or aggregation group. | DWS window KPI / PBI views | no | Data completeness and density | 待 cluster 验证 |
| `failure_sample_count` | Count of records inside configured failure windows. | DWS window KPI / PBI views | target summary only | Failure-window density | 待 cluster 验证 |
| `failure_window_rate` | Failure-window sample ratio in a group. | DWS window KPI / sensor KPI / PBI views | target summary only | Failure contrast and table sorting | 待 cluster 验证 |
| `failure_window` | Weak flag for configured failure intervals. | P9 label builder | target/grouping only | Failure-window marker | Checked locally; cluster parity pending |
| `pre_failure_1h` | Weak flag for 1 hour before configured failure start. | P9 label builder | target/grouping only | Early-warning context | Checked locally; cluster parity pending |
| `pre_failure_6h` | Weak flag for 6 hours before configured failure start. | P9 label builder | target/grouping only | Early-warning context | Checked locally; cluster parity pending |
| `pre_failure_24h` | Weak flag for 24 hours before configured failure start. | P9 label builder | target | Main baseline target | Checked locally; cluster parity pending |
| `post_maintenance` | Pragmatic recovery window after configured failure end. | P9 label builder | exclusion mask only | Recovery context | Checked locally; cluster parity pending |
| `normal_candidate` | Conservative candidate normal flag. | P9 label builder | sampling/mask only | Normal-vs-failure comparison | Checked locally; cluster parity pending |
| `rul_seconds` | Seconds until next configured failure start; weak target only. | P9 label builder | target only | RUL analysis candidate | Must not be model feature |
| `tp2` / `avg_tp2` | Compressor-side pressure. | ODS / DWS / P9 features | yes | Pressure trend and model signal | 待 cluster 验证 |
| `tp3` / `avg_tp3` | Pneumatic-panel pressure. | ODS / DWS / P9 features | yes | Pressure-balance trend | 待 cluster 验证 |
| `h1` | Cyclonic-separator pressure drop. | ODS / P9 features | yes | Discharge behavior | 待 cluster 验证 |
| `dv_pressure` | Air-dryer tower discharge pressure. | ODS / P9 features | yes | Dryer discharge diagnostics | 待 cluster 验证 |
| `reservoirs` / `avg_reservoirs` | Reservoir downstream pressure. | ODS / DWS / P9 features | yes | Pressure-balance trend | 待 cluster 验证 |
| `oil_temperature` / `avg_oil_temperature` | Compressor oil temperature. | ODS / DWS / P9 features | yes | Thermal stress and failure contrast | 待 cluster 验证 |
| `motor_current` / `avg_motor_current` | Motor phase current. | ODS / DWS / P9 features | yes | State and load behavior | 待 cluster 验证 |
| `comp` | Air-intake valve electrical signal. | ODS / P9 features | yes | Digital activity and state machine | 待 cluster 验证 |
| `dv_electric` | Standardized field for raw `DV_eletric`. | ODS / P9 features | yes | Outlet valve signal | 待 cluster 验证 |
| `towers` | Dryer tower operating/draining signal. | ODS / P9 features | yes | Dryer-cycle behavior | 待 cluster 验证 |
| `mpg` | Intake-valve load activation signal. | ODS / P9 features | yes | Load demand behavior | 待 cluster 验证 |
| `lps` | Low-pressure switch signal. | ODS / P9 features | yes | Rare low-pressure events | 待 cluster 验证 |
| `pressure_switch` | Air-drying tower discharge switch. | ODS / P9 features | yes | Dryer-discharge diagnostics | 待 cluster 验证 |
| `oil_level` | Oil-level digital signal. | ODS / P9 features | yes | Maintenance/safety context | 待 cluster 验证 |
| `caudal_impulses` | Air-flow pulse signal. | ODS / P9 features | yes | Flow activity | 待 cluster 验证 |
| `delta_tp3_reservoirs` | Pressure balance: `tp3 - reservoirs`. | P9 features | yes | Pressure-balance anomaly | Checked locally |
| `delta_tp2_tp3` | Pressure generation gap: `tp2 - tp3`. | P9 features | yes | Compressor pressure behavior | Checked locally |
| `active_count_*` | Digital signal active count in a minute/window. | P9 features | yes | Digital activation intensity | Checked locally |
| `toggle_count_*` | Digital signal switch count in a minute/window. | P9 features | yes | State switching frequency | Checked locally |
| `state_transition_count` | Operating-state transition count. | P9 features | yes | State instability view | Checked locally |
| `roll5_*`, `roll15_*`, `roll60_*` | Right-aligned rolling features. | P9 features | yes | Multi-scale trend and model input | Checked locally |
| `model_name` | Baseline model identifier. | `p9_model_metrics.json` | no | Model comparison | Checked locally |
| `precision`, `recall`, `f1`, `pr_auc` | Classification metrics. | `p9_model_metrics.json` | no | Baseline QA | Checked locally |
| `false_alarms_per_day` | Early-warning false-alarm burden. | `p9_model_metrics.json` | no | Alarm usability | Checked locally |
| `lead_time_hours` | Detected failure lead time. | `p9_model_metrics.json` | no | Early-warning value | Checked locally |
| `risk_score` | Baseline model score from prediction sample. | `p9_model_prediction_sample.tsv` | no | Risk timeline | Checked locally |

## Dashboard Filters

| Filter | Values | Notes |
| --- | --- | --- |
| Date range | `event_time`, `event_minute`, `dt` | Default to full P9 range, then allow failure-window zoom. |
| State | `loaded`, `unloaded`, `stopped` | Derived from `motor_current`; state semantics should be shown in field help. |
| Failure context | normal candidate, pre-failure windows, failure window, post-maintenance | Labels are weak and should be visually separated from verified alarms. |
| Sensor type | analog, digital | Use sensor dictionary as the canonical source. |
| Validation level | worker local, local data, cluster | Prevent mixing local evidence with cluster evidence. |

## Query Samples

These samples were prepared by worker for cluster-side verification. They are not worker-run evidence; they were verified by master in P12 via `bin/p12_query_layer_validation.sh`.

### Hive BI Views

```sql
USE metropt_quality;

SELECT
  dt,
  operating_state,
  COUNT(*) AS minute_count,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count,
  AVG(failure_window_rate) AS avg_failure_window_rate,
  AVG(avg_oil_temperature) AS avg_oil_temperature,
  AVG(avg_motor_current) AS avg_motor_current
FROM vw_pbi_metropt_window_kpi
GROUP BY dt, operating_state
ORDER BY dt, operating_state
LIMIT 100;
```

```sql
USE metropt_quality;

SELECT
  sensor_name,
  sensor_type,
  unit,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM vw_pbi_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;
```

### Trino / Iceberg

```sql
SELECT COUNT(*) AS ods_rows
FROM iceberg.metropt_quality_iceberg.ods_metropt_readings;
```

```sql
SELECT
  sensor_name,
  sensor_type,
  COUNT(*) AS rows_in_long_table
FROM iceberg.metropt_quality_iceberg.dwd_metropt_sensor_long
GROUP BY sensor_name, sensor_type
ORDER BY sensor_name;
```

### Doris

```sql
USE metropt_quality_olap;

SELECT
  sensor_name,
  sensor_type,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM dws_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;
```

## BI Boundary

- The local validation pass did not run Hive, Trino, Doris, Spark-on-YARN, Kafka, Flink, or Redis in the original P9 round.
- BI query samples have cluster validation evidence in P12; future query changes must still record command, return code, output, and run path.
- P9 feature fields are analysis-side fields and are not currently accepted DWS/Hive production fields unless master decides to promote them.
- Label fields and `rul_seconds` are target/evaluation fields, not model features.

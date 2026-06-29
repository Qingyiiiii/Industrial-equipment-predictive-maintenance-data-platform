# P9 Feature Parity Report

## Scope

- Run id: `20260606_220742`.
- Overall status: `PASS`.
- Warehouse feature table: `data/metropt_quality/analysis/models/p9_window_features_1min_warehouse.parquet`.
- Warehouse feature sample: `data/metropt_quality/analysis/models/p9_window_features_1min_warehouse_sample.tsv`.
- CSV-derived reference: `data/metropt_quality/analysis/models/p9_window_features_1min.parquet`.

This report upgrades P9 feature generation from CSV-derived analysis to warehouse-derived analysis by reading accepted HDFS ODS/DWD/DWS Parquet paths. It does not overwrite the original P9 CSV-derived feature table.

## Warehouse Sources

| Source | Path | Rows | Distinct count |
| --- | --- | ---: | ---: |
| ods_readings | `hdfs:///lakehouse/projects/metropt_quality/ods/readings` | 1516948 | 252720 |
| dwd_sensor_long | `hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long` | 22754220 | 15 |
| dws_window_kpi | `hdfs:///lakehouse/projects/metropt_quality/dws/window_kpi` | 269991 | 252720 |
| dws_sensor_kpi | `hdfs:///lakehouse/projects/metropt_quality/dws/sensor_kpi` | 15 | 15 |

## Feature Shape

| Source | Rows | Columns | Min event minute | Max event minute |
| --- | ---: | ---: | --- | --- |
| CSV-derived | 252720 | 507 | 2020-02-01 00:00:00 | 2020-09-01 03:59:00 |
| warehouse-derived | 252720 | 507 | 2020-02-01 00:00:00 | 2020-09-01 03:59:00 |

## Parity Checks

| Check | Status | Detail |
| --- | --- | --- |
| shape_parity | PASS | Warehouse-derived and CSV-derived feature tables have identical row and column counts. |
| time_range_parity | PASS | Time ranges match exactly. |
| label_distribution_parity | PASS | P9 label positive-row counts match exactly. |
| ods_minute_alignment | PASS | Warehouse feature row count matches ODS distinct event_minute count. |
| dws_sample_count_alignment | PASS | DWS window sample_count sum matches ODS row count. |

## Label Distribution

| Label | CSV positive rows | Warehouse positive rows | Delta |
| --- | ---: | ---: | ---: |
| failure_window | 4959 | 4959 | 0 |
| pre_failure_1h | 240 | 240 | 0 |
| pre_failure_6h | 1440 | 1440 | 0 |
| pre_failure_24h | 4897 | 4897 | 0 |
| post_maintenance | 4170 | 4170 | 0 |
| normal_candidate | 238695 | 238695 | 0 |

## Key Statistic Mean Comparison

| Column | CSV mean | Warehouse mean | Delta |
| --- | ---: | ---: | ---: |
| failure_window | 0.019622507122507123 | 0.019622507122507123 | 0.0 |
| mean_delta_tp2_tp3 | -7.600038528442383 | -7.600038588989886 | -6.054750301132117e-08 |
| mean_delta_tp3_reservoirs | -0.0006192834698595107 | -0.0006192849794238735 | -1.5095643628344757e-09 |
| mean_motor_current | 2.0562005043029785 | 2.056200328827123 | -1.7547585562383006e-07 |
| mean_oil_temperature | 62.642948150634766 | 62.64295462958252 | 6.478947753407738e-06 |
| mean_reservoirs | 8.981539726257324 | 8.98154077718235 | 1.0509250252255242e-06 |
| mean_tp2 | 1.380882740020752 | 1.3808829032130425 | 1.6319229056982465e-07 |
| mean_tp3 | 8.980921745300293 | 8.980921492202926 | -2.5309736706446984e-07 |
| normal_candidate | 0.9445037986704653 | 0.9445037986704653 | 0.0 |
| post_maintenance | 0.016500474833808166 | 0.016500474833808166 | 0.0 |
| pre_failure_24h | 0.019377176321620768 | 0.019377176321620768 | 0.0 |
| sample_count | 6.002484963596075 | 6.002484963596075 | 0.0 |
| state_transition_count | 0.08269626464070909 | 0.08269626464070909 | 0.0 |

## Decision Boundary

- Warehouse-derived P9 feature table is stored separately from the P9 CSV-derived feature table.
- Label columns are retained for evaluation and parity checks only.
- P10 model reruns must explicitly exclude `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` from model features.

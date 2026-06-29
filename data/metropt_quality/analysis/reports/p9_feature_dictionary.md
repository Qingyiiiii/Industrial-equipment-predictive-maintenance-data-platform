# P9 Feature Dictionary

## Generated Feature Table

- Full local feature table: `data/metropt_quality/analysis/models/p9_window_features_1min.parquet`.
- Feature sample: `data/metropt_quality/analysis/models/p9_window_features_1min_sample.tsv`.
- Feature dictionary TSV: `data/metropt_quality/analysis/models/p9_feature_dictionary.tsv`.
- Rows: `252720` minute-grain records.
- Columns: `507`.

## Design Rules

- Windows are right-aligned at 1min, 5min, 15min, and 60min scales.
- Rolling features use current and past minute buckets only.
- Pressure difference features include `tp3 - reservoirs` and `tp2 - tp3`.
- Digital features include activation counts and toggle counts.
- State features include state duration counts and state transition counts.
- Label fields are retained for offline evaluation, but they are explicitly marked as leakage-risk fields and must not enter model features.

## Feature Groups

| Group | Examples | Leakage risk |
| --- | --- | --- |
| Raw minute analog statistics | `mean_tp2`, `std_oil_temperature`, `slope_motor_current` | no |
| Multi-scale rolling statistics | `roll15_mean_mean_tp2`, `roll60_std_mean_oil_temperature` | no |
| Pressure deltas | `mean_delta_tp3_reservoirs`, `roll15_mean_mean_delta_tp2_tp3` | no |
| Digital activity | `active_count_dv_electric`, `toggle_count_comp` | no |
| Operating state | `state_loaded_count`, `state_transition_count` | no |
| Labels and masks | `pre_failure_24h`, `rul_seconds` | yes, target/grouping only |

## Validation Boundary

This local artifact is generated from local CSV. Cluster validation should regenerate or compare it against the cluster ODS/DWS outputs before using it as an accepted project feature table.

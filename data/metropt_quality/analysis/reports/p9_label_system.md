# P9 Label System

## Source and Boundary

- Label source: `metropt.failure_windows` in the active MetroPT config.
- Local row count checked from CSV timestamps: `1516948`.
- Time range: `2020-02-01 00:00:00` to `2020-09-01 03:59:50`.
- Label summary artifact: `data/metropt_quality/analysis/models/p9_label_summary.tsv`.

The original MetroPT-3 dataset is unlabeled at row level. The company failure reports provide time intervals that can support failure prediction, anomaly detection, and RUL experiments, but they are weak labels. They must not be described as manually verified per-row fault truth.

## Configured Failure Windows

| Failure id | Start | End | Failure type | Severity | Label source |
| --- | --- | --- | --- | --- | --- |
| 1 | `2020-04-18 00:00:00` | `2020-04-18 23:59:59` | `air_leak_high_stress` | `high_stress` | Configured official failure interval |
| 2 | `2020-05-29 23:30:00` | `2020-05-30 06:00:00` | `air_leak_high_stress` | `high_stress` | Configured official failure interval |
| 3 | `2020-06-05 10:00:00` | `2020-06-07 14:30:00` | `air_leak_high_stress` | `high_stress` | Configured official failure interval |
| 4 | `2020-07-15 14:30:00` | `2020-07-15 19:00:00` | `air_leak_high_stress` | `high_stress` | Configured official failure interval |

## Label Rules

| Label | Rule | Intended use | Leakage control | Limitation |
| --- | --- | --- | --- | --- |
| `failure_window` | `event_time` falls inside one configured failure interval. | Current-window weak classification target and EDA grouping. | Use only as target/grouping, never as feature. | Interval-derived weak label, not manually verified row-level truth. |
| `pre_failure_1h` | `event_time` is in `[failure_start - 1h, failure_start)`. | Short-horizon early-warning target. | Computed from event calendar for offline labels; not available as online feature. | Positive windows are nested inside 6h and 24h labels. |
| `pre_failure_6h` | `event_time` is in `[failure_start - 6h, failure_start)`. | Medium-horizon early-warning target. | Use time split before model evaluation. | Still weak because the failure report has interval granularity. |
| `pre_failure_24h` | `event_time` is in `[failure_start - 24h, failure_start)`. | Main P9 early-warning baseline target. | Exclude the label and all future-derived fields from feature columns. | Includes normal-looking minutes near a reported failure. |
| `post_maintenance` | `event_time` is in `(failure_end, failure_end + 24h]`. | Recovery/maintenance context exclusion flag. | Do not train normal class from recovery windows unless explicitly tested. | It is a pragmatic recovery window, not a verified maintenance work order. |
| `normal_candidate` | Not `failure_window`, not `pre_failure_24h`, and not `post_maintenance`. | Conservative normal class candidate. | Still split by time; do not sample randomly across months. | It is only a candidate normal label because the dataset is unlabeled. |
| `rul_seconds` | Seconds until the next configured failure start; `0` inside a configured failure window; null after the last known failure horizon. | Weak RUL regression target or report field. | Target only; never use as a model feature. | It is derived from failure intervals and is not true component remaining life. |

## Local Label Distribution

| Label | Positive rows | Positive rate |
| --- | ---: | ---: |
| `failure_window` | 29960 | 0.019750 |
| `pre_failure_1h` | 1387 | 0.000914 |
| `pre_failure_6h` | 8327 | 0.005489 |
| `pre_failure_24h` | 28668 | 0.018898 |
| `post_maintenance` | 25193 | 0.016608 |
| `normal_candidate` | 1433127 | 0.944744 |

## Time Split Requirement

- Train/validation/test must be split by `event_time`, not random rows.
- Recommended P9 split for full CSV experiments:
  - train: before `2020-06-01 00:00:00`
  - validation: `2020-06-01 00:00:00` to before `2020-07-01 00:00:00`
  - test: from `2020-07-01 00:00:00`
- The split keeps earlier failure windows in train/validation and leaves the July failure for test-style evaluation.

## Leakage Notes

- `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` are labels or evaluation masks, not feature inputs.
- Rolling features must be right-aligned and computed only from current/past sensor values.
- `rul_seconds` uses knowledge of the next configured failure start, so it is acceptable only as an offline target.
- Local label generation is a local self-check; cluster table alignment is 待 cluster 验证.

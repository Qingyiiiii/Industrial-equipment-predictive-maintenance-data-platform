# MetroPT Baseline Model Report

## Dataset

- Source: DWS window KPI.
- Rows: `269991`.
- Train rows: `188993`.
- Test rows: `80998`.
- Positive rows: `4960`.
- Negative rows: `265031`.
- Split strategy: time-ordered 70/30 split by `event_minute`.

The target is derived from `failure_sample_count > 0` or `failure_window_rate > 0`. This is an interval-derived failure-window label, not a manually verified row-level fault label.

## Metrics

- `logistic_regression`: accuracy `0.9956`, precision `0.0426`, recall `0.0148`, F1 `0.0219`
- `random_forest`: accuracy `0.9967`, precision `0.0000`, recall `0.0000`, F1 `0.0000`

## Top Feature Signals

- `logistic_regression` / `active_count_dv_electric`: `-4.422432`
- `logistic_regression` / `active_count_comp`: `-4.345559`
- `logistic_regression` / `avg_tp2`: `3.981001`
- `logistic_regression` / `max_h1`: `2.719830`
- `logistic_regression` / `sample_count`: `2.536755`
- `logistic_regression` / `avg_dv_pressure`: `2.524380`
- `logistic_regression` / `max_tp2`: `2.420735`
- `logistic_regression` / `min_h1`: `2.371550`
- `logistic_regression` / `min_tp2`: `-2.335733`
- `logistic_regression` / `max_dv_pressure`: `-2.269402`
- `logistic_regression` / `std_h1`: `-1.700074`
- `logistic_regression` / `avg_motor_current`: `1.503319`

## Limitations

- This is a baseline only; it should not be presented as a production predictive-maintenance model.
- Class imbalance can make accuracy misleading.
- Time leakage is controlled with a time-based split, but feature windows still need review before any future horizon-based prediction task.

## Figures

- `data/metropt_quality/analysis/figures/baseline_confusion_matrices.png`
- `data/metropt_quality/analysis/figures/logistic_regression_feature_signals.png`
- `data/metropt_quality/analysis/figures/random_forest_feature_signals.png`

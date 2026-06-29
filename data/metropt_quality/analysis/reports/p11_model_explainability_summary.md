# P11 Model Explainability Summary

## Scope

- Metrics source: `data/metropt_quality/analysis/models/p9_model_metrics.json`.
- Logistic weights source: `data/metropt_quality/analysis/models/p9_logistic_feature_weights.tsv`.
- P10 comparison source: `data/metropt_quality/analysis/models/p10_model_metric_comparison.tsv`.

This report explains existing baseline artifacts. It does not train a new model and does not promote the realtime signal proxy into a production ML model.

## Model Metrics

- `isolation_forest`: precision `0.022631904794661897`, recall `0.9657534246575342`, F1 `0.044227365402280824`, false alarms/day `676.5714285714286`
- `numpy_logistic_regression`: precision `0.0024102523906568427`, recall `0.12035225048923678`, F1 `0.00472586160525608`, false alarms/day `808.0793650793651`
- `random_forest`: precision `0.006846081208687441`, recall `0.02837573385518591`, F1 `0.01103081019399011`, false alarms/day `66.77777777777777`
- `robust_anomaly_score`: precision `0.030268953237747167`, recall `0.9481409001956947`, F1 `0.05866505221734524`, false alarms/day `492.76190476190476`

## Top Logistic Features

| Feature | Coefficient | Abs coefficient |
| --- | ---: | ---: |
| `roll60_max_toggle_count_dv_electric` | -0.29763893 | 0.29763893 |
| `roll60_std_mean_delta_tp3_reservoirs` | 0.26307100 | 0.26307100 |
| `roll60_std_mean_motor_current` | 0.21056630 | 0.21056630 |
| `roll15_std_mean_delta_tp3_reservoirs` | 0.19446128 | 0.19446128 |
| `roll60_std_mean_reservoirs` | -0.18493295 | 0.18493295 |
| `roll60_std_mean_tp3` | -0.18302812 | 0.18302812 |
| `roll60_max_mean_oil_temperature` | 0.18301030 | 0.18301030 |
| `roll60_std_mean_tp2` | 0.18218462 | 0.18218462 |
| `roll60_std_mean_delta_tp2_tp3` | 0.18120248 | 0.18120248 |
| `roll15_std_mean_motor_current` | 0.17779974 | 0.17779974 |
| `roll15_mean_active_count_pressure_switch` | -0.12724994 | 0.12724994 |
| `roll60_max_toggle_count_oil_level` | 0.12473765 | 0.12473765 |
| `roll60_std_mean_h1` | 0.11911613 | 0.11911613 |
| `roll15_max_toggle_count_pressure_switch` | -0.11880259 | 0.11880259 |
| `roll60_std_mean_dv_pressure` | -0.11019001 | 0.11019001 |

## Interpretation Notes

- Positive coefficients increase the weak-label `pre_failure_24h` score in the numpy logistic baseline.
- Feature weights are meaningful only under the current chronological split and weak-label definition.
- Lead time is reported only for `numpy_logistic_regression`; RF/IF metrics do not carry lead-time evidence in this run.
- Any future RUL or anomaly-detection extension must keep `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` out of model features.

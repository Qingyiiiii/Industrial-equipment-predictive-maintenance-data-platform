# MetroPT Multidimensional Analysis Report

## Executive Summary

- Data time range: `2020-01-31 08:00:00` to `2020-08-31 12:59:50`.
- Active days: `212`.
- ODS samples: `1516948`.
- Window KPI rows: `269991`.
- Sensor KPI rows: `15`.
- Failure-window samples: `29960`.
- Failure-window rate: `0.019750`.

The failure label used here is derived from the official failure time intervals configured in the project. It is not a manually verified row-level fault label, so model and analysis results should be treated as baseline evidence rather than proof of causal failure behavior.

## Operating State

- `loaded`: samples `112`, failure rate `0.000000`, avg current `7.8282`
- `stopped`: samples `829000`, failure rate `0.000112`, avg current `0.0410`
- `unloaded`: samples `687836`, failure rate `0.043422`, avg current `4.4708`

## Sensors Worth Tracking First

- `oil_temperature`: failure-window mean shift 13.214833; std 6.516261
- `h1`: failure-window mean shift -7.682221; std 3.333200
- `tp2`: failure-window mean shift 6.883500; std 3.250930

## Failure Window Contrast

Top average shifts between failure-window and normal-window samples:

- `oil_temperature`: failure-normal avg delta `13.214833`
- `h1`: failure-normal avg delta `-7.682221`
- `tp2`: failure-normal avg delta `6.883500`
- `motor_current`: failure-normal avg delta `3.552240`
- `dv_pressure`: failure-normal avg delta `1.840203`
- `dv_electric`: failure-normal avg delta `0.851092`
- `comp`: failure-normal avg delta `-0.849291`
- `mpg`: failure-normal avg delta `-0.844912`
- `tp3`: failure-normal avg delta `-0.710466`
- `reservoirs`: failure-normal avg delta `-0.709583`

These sensors are candidates for monitoring and feature engineering. They should be validated again after the full VM run because current results depend on the generated ODS/DWD/DWS artifacts.

## Modeling Boundary

The first model version should remain a baseline. Class imbalance, time leakage, and interval-derived labels are the main risks. Any reported accuracy must be read together with recall, F1, the confusion matrix, and the time-based train/test split.

## Figures

- `data/metropt_quality/analysis/figures/multidim_daily_samples_failure_trend.png`
- `data/metropt_quality/analysis/figures/operating_state_distribution.png`
- `data/metropt_quality/analysis/figures/sensor_mean_volatility_ranking.png`
- `data/metropt_quality/analysis/figures/failure_window_sensor_contrast.png`
- `data/metropt_quality/analysis/figures/pressure_current_temperature_timeseries.png`
- `data/metropt_quality/analysis/figures/sensor_correlation_heatmap.png`

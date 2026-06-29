# P9 Deep EDA Report

## Data Scope

- Source: full local CSV through project config.
- Row count: `1516948`.
- Time range: `2020-02-01 00:00:00` to `2020-09-01 03:59:50`.
- Active days: `212`.
- Failure-window rows: `29960`.
- Failure-window rate: `0.019750`.
- Minute feature rows generated: `252720`.

The failure-window fields are interval-derived weak labels, not manually verified row-level fault labels. Local results are based on CSV analysis and remain 待 cluster 验证 for cluster Parquet alignment.

## Sampling and Breaks

- Sampling interval seconds: min `8.0`, median `10.0`, average `12.1412`, max `172918.0`.
- Top sampling gaps:

- `2020-04-27 01:12:49` after `2020-04-25 01:10:51`: `172918.0` seconds
- `2020-06-28 23:07:43` after `2020-06-27 10:53:07`: `130476.0` seconds
- `2020-03-01 04:00:09` after `2020-02-28 23:57:08`: `100981.0` seconds
- `2020-08-05 08:23:01` after `2020-08-04 07:42:28`: `88833.0` seconds
- `2020-05-25 01:14:14` after `2020-05-24 00:39:23`: `88491.0` seconds
- `2020-07-08 15:20:51` after `2020-07-07 15:24:51`: `86160.0` seconds
- `2020-08-23 18:51:01` after `2020-08-22 19:11:44`: `85157.0` seconds
- `2020-05-10 22:48:58` after `2020-05-10 00:31:17`: `80261.0` seconds
- `2020-06-08 11:48:04` after `2020-06-07 14:19:39`: `77305.0` seconds
- `2020-04-02 09:59:17` after `2020-04-01 13:15:40`: `74617.0` seconds

## Failure-Window and Pre-Failure Sensor Contrast

- `failure_window` / `oil_temperature`: mean `75.598022`, delta vs normal `13.287632`
- `failure_window` / `h1`: mean `0.037659`, delta vs normal `-7.691601`
- `failure_window` / `tp2`: mean `8.115376`, delta vs normal `6.891512`
- `failure_window` / `motor_current`: mean `5.532253`, delta vs normal `3.559765`
- `pre_failure_1h` / `oil_temperature`: mean `65.321091`, delta vs normal `3.010700`
- `post_maintenance` / `oil_temperature`: mean `65.307266`, delta vs normal `2.996876`
- `pre_failure_1h` / `h1`: mean `5.758787`, delta vs normal `-1.970473`
- `failure_window` / `dv_pressure`: mean `1.859815`, delta vs normal `1.840865`
- `pre_failure_1h` / `tp2`: mean `3.056893`, delta vs normal `1.833029`
- `pre_failure_6h_only` / `oil_temperature`: mean `63.643955`, delta vs normal `1.333565`
- `pre_failure_24h_only` / `oil_temperature`: mean `63.259941`, delta vs normal `0.949551`
- `failure_window` / `tp3`: mean `8.288177`, delta vs normal `-0.712397`
- `failure_window` / `reservoirs`: mean `8.289664`, delta vs normal `-0.711512`
- `pre_failure_1h` / `motor_current`: mean `2.649737`, delta vs normal `0.677248`
- `pre_failure_6h_only` / `h1`: mean `7.221980`, delta vs normal `-0.507280`
- `pre_failure_6h_only` / `tp2`: mean `1.628916`, delta vs normal `0.405052`
- `post_maintenance` / `motor_current`: mean `2.227353`, delta vs normal `0.254865`
- `pre_failure_6h_only` / `motor_current`: mean `2.206718`, delta vs normal `0.234229`

## Sensor Volatility Ranking

- `oil_temperature`: std `6.514923`, mean `62.644184`
- `h1`: std `3.331024`, mean `7.568155`
- `delta_tp2_tp3`: std `3.319170`, mean `-7.616785`
- `tp2`: std `3.257842`, mean `1.367826`
- `motor_current`: std `2.301805`, mean `2.050171`
- `tp3`: std `0.638840`, mean `8.984611`
- `reservoirs`: std `0.637983`, mean `8.985233`
- `dv_pressure`: std `0.379557`, mean `0.055956`
- `delta_tp3_reservoirs`: std `0.002451`, mean `-0.000623`

## Correlation Highlights

- `tp3` / `reservoirs`: `1.0000`
- `h1` / `delta_tp2_tp3`: `-0.9845`
- `tp2` / `delta_tp2_tp3`: `0.9813`
- `tp2` / `h1`: `-0.9611`
- `tp2` / `motor_current`: `0.6975`
- `motor_current` / `delta_tp2_tp3`: `0.6032`
- `h1` / `motor_current`: `-0.6001`
- `oil_temperature` / `motor_current`: `0.5321`
- `dv_pressure` / `delta_tp2_tp3`: `0.4381`
- `h1` / `dv_pressure`: `-0.4270`

## Business-Useful Signals

- `oil_temperature` should remain a first-tier feature because thermal drift is directly tied to compressor stress and it ranks high in failure/pre-failure contrasts.
- `tp3 - reservoirs` is a pressure-balance feature with clear physical meaning: TP3 and reservoir pressure should be close during stable operation, so widening gaps can indicate air delivery or leak behavior.
- `motor_current` plus `COMP`/`DV_eletric`/`MPG` activation and toggle counts capture the compressor state machine better than any single raw digital signal.

## Figures

- `data/metropt_quality/analysis/figures/p9_daily_sample_failure_trend.png`
- `data/metropt_quality/analysis/figures/p9_pre_failure_sensor_delta.png`
- `data/metropt_quality/analysis/figures/p9_sensor_correlation_heatmap.png`
- `data/metropt_quality/analysis/figures/p9_pressure_current_oil_fault_timeline.png`
- `data/metropt_quality/analysis/figures/p9_state_transition_frequency.png`

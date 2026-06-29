# MetroPT Data Quality Report

## Summary

- Row count: `1516948`.
- Expected row count: `1516948`.
- Row count matches expected: `True`.
- Time range: `2020-01-31 08:00:00` to `2020-08-31 12:59:50`.
- Duplicate event_time count: `0`.
- Sampling interval seconds: min `8`, avg `12.1412`, max `172918`.
- Failure-window labels are derived from configured official failure intervals; they are not manually verified row-level labels.

## Daily Sample Quality

- Active days: `212`.
- Median daily samples: `7435`.
- Anomalous days by median +/-20% rule: `35`.

- 2020-02-06: 4939 samples
- 2020-02-24: 5893 samples
- 2020-03-07: 3043 samples
- 2020-03-11: 5226 samples
- 2020-03-15: 3746 samples
- 2020-03-28: 3536 samples
- 2020-03-30: 5907 samples
- 2020-04-01: 4816 samples
- 2020-04-02: 3853 samples
- 2020-04-13: 3501 samples

## Null Counts

All configured analog and digital sensors are checked after ODS normalization.

```json
{'tp2': 0, 'tp3': 0, 'h1': 0, 'dv_pressure': 0, 'reservoirs': 0, 'oil_temperature': 0, 'motor_current': 0, 'comp': 0, 'dv_electric': 0, 'towers': 0, 'mpg': 0, 'lps': 0, 'pressure_switch': 0, 'oil_level': 0, 'caudal_impulses': 0}
```

## Digital Sensor Values

- `comp`: {'0.0': 247328, '1.0': 1269620}
- `dv_electric`: {'0.0': 1273310, '1.0': 243638}
- `towers`: {'0.0': 121586, '1.0': 1395362}
- `mpg`: {'0.0': 253840, '1.0': 1263108}
- `lps`: {'0.0': 1511760, '1.0': 5188}
- `pressure_switch`: {'0.0': 12990, '1.0': 1503958}
- `oil_level`: {'0.0': 145391, '1.0': 1371557}
- `caudal_impulses`: {'0.0': 95406, '1.0': 1421542}

## Figures

- `data/metropt_quality/analysis/figures/daily_sample_count_trend.png`

# src Offline Pipeline

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

`src/` contains the main MetroPT-3 offline lakehouse pipeline. It transforms the raw CSV into ODS, DWD, DWS, Hive/Iceberg tables, and BI-friendly views.

## Execution Order

Use the unified entry point first:

```bash
cd /home/common/tmp/pycharm_Design
export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
python src/run_metropt_offline.py
```

Run only up to DWS:

```bash
python src/run_metropt_offline.py --stop-after 04_metropt_kpi_calc.py
```

Resume from a specific step:

```bash
python src/run_metropt_offline.py --start-at 03_metropt_dwd_sensor_long.py
```

Logs are written to:

```text
data/metropt_quality/logs/<run_id>/
```

Start with `offline_run_summary.tsv`, then inspect the `.log` file for any failed step.

## Files

| File | Purpose | Input | Output |
| --- | --- | --- | --- |
| `00_metropt_preflight.py` | Preflight check for config, CSV, fields, HDFS paths, and Spark readability | `METROPT_CONFIG`, raw CSV | Console checks; blocks downstream steps on failure |
| `01_metropt_profile.py` | Raw CSV profiling and basic quality checks | HDFS raw CSV | Profile report, row count, fields, time range |
| `02_metropt_csv_to_parquet.py` | Standardizes CSV into ODS Parquet | Raw CSV | `ods/readings` |
| `03_metropt_dwd_sensor_long.py` | Expands wide sensor columns into long format | ODS Parquet | `dwd/sensor_long` |
| `04_metropt_kpi_calc.py` | Computes offline DWS KPI datasets | ODS/DWD | `dws/overall_kpi`, `dws/window_kpi`, `dws/sensor_kpi` |
| `05_metropt_to_hive_iceberg.py` | Publishes ODS/DWD/DWS to Hive and optional Iceberg | Parquet datasets | Hive tables and Iceberg tables |
| `06_metropt_hive_views.py` | Creates BI-friendly Hive views | Hive DWS tables | BI views |
| `run_metropt_offline.py` | Runs `00 -> 06` and records logs plus return codes | Pipeline scripts | `offline_run_summary.tsv` and step logs |
| `metropt_utils.py` | Shared config, Spark, field normalization, and path helpers | Imported by scripts | Not intended to run directly |

## Data Layers

| Layer | Meaning | Typical path |
| --- | --- | --- |
| Raw | Original CSV | `hdfs:///lakehouse/projects/metropt_quality/raw/MetroPT3_AirCompressor.csv` |
| ODS | Standardized wide readings table | `hdfs:///lakehouse/projects/metropt_quality/ods/readings` |
| DWD | Sensor long table | `hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long` |
| DWS | KPI summary layer | `hdfs:///lakehouse/projects/metropt_quality/dws/*` |
| Hive/Iceberg | Query publication layer | `metropt_quality.*`, `metropt_quality_iceberg.*` |

## How To Read Results

1. Check whether all steps in `offline_run_summary.tsv` have `return_code=0`.
2. Confirm ODS row count is around `1,516,948`.
3. Confirm DWD row count is around `22,754,220`.
4. Confirm DWS sensor KPI covers 15 sensors.
5. Confirm Hive tables are queryable.

## Common Issues

| Symptom | Action |
| --- | --- |
| CSV not found | Check the raw path in `METROPT_CONFIG` and confirm the file exists in HDFS |
| Spark failure | Read the step `.log`, then inspect YARN application logs |
| Slow Hive COUNT or JDK issue | Use `bin/metropt_hive_mr_count_check.sh --mode offline` |
| Pipeline fails halfway | Fix the failed step first, then resume with `--start-at` |
| Field-name mismatch | Run `00_metropt_preflight.py`; it checks required fields and known spelling differences |


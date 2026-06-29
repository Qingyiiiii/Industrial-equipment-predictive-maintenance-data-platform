# Lightweight Test Notes

Language / 语言: [中文](README_zh.md) | [English](README_en.md)

This directory contains local tests that do not depend on Spark, Flink, Kafka, Redis, or Hive. They quickly validate that the core project contracts have not drifted.

Run:

```powershell
python bin/local_code_quality_check.py
```

## Check Scope

- Python AST syntax checks.
- Config, path, failure-window, and field-mapping helpers in `src/metropt_utils.py`.
- Kafka JSON contract, DLQ error classification, and `risk_score` signal-proxy boundary in `streaming/metropt_realtime_risk_score_plan.py`.
- Key config items in `config/metropt_quality.local.yaml`.

## Out Of Scope

- Does not start Spark.
- Does not connect to Kafka, Flink, Redis, or Hive.
- Does not generate Parquet, figures, or model files.


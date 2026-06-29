# P9 Cluster Validation Checklist

## Scope

This checklist gives a reproducible path to validate P9 outputs. It was prepared from local analysis evidence, so every Spark/Hive/HDFS/Trino/Doris/Kafka/Flink/Redis result below remains 待 cluster 验证 until cluster-side logs and return codes are recorded.

## Recommended Order

### 1. Check Python Dependencies

```bash
cd /home/common/tmp/pycharm_Design
python - <<'PY'
import importlib.util
for name in ["pandas", "pyarrow", "sklearn"]:
    status = "OK" if importlib.util.find_spec(name) else "MISSING"
    print(f"{name}\t{status}")
PY
```

Expected output:

```text
pandas	OK
pyarrow	OK
sklearn	OK
```

Validation boundary:

- `pandas` is required by P9 label, feature, and model scripts.
- `pyarrow` is required for reading and writing `p9_window_features_1min.parquet`.
- `sklearn` is required to train Random Forest and Isolation Forest. If it is missing or training fails because of the local runtime, those models must remain explicitly `skipped` with the recorded reason; do not mark that as a model-quality failure.
- If `pandas` or `pyarrow` is missing, fix the Python environment before judging P9 feature/model reruns.

### 2. Read Current Evidence Reports

```bash
cd /home/common/tmp/pycharm_Design
ls data/metropt_quality/analysis/reports/p9_*.md
```

Expected focus:

- Check the change summaries.
- Confirm P9 changes are analysis/documentation side.
- Confirm P9 realtime-risk work is still design/dry-run scope unless master implements and validates Kafka/Flink/Redis integration.
- Confirm P8 delivery package was not overwritten.

### 3. Run First Readiness Check

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh
```

Expected output:

- A new `data/metropt_quality/validation_runs/p7_ops_snapshot_<run_id>/` directory.
- `ops_snapshot.md`, `ops_snapshot.json`, `readiness.tsv`, `service_status.tsv`, and `host_metrics.tsv`.
- Any WARN must be described as resource/readiness context, not silently ignored.

### 4. Static-Check Analysis Code

```bash
cd /home/common/tmp/pycharm_Design
python -m compileall analysis streaming
```

Expected output:

- Return code `0`.
- If `.pyc` write permission blocks the command, run a documented no-pyc syntax check and record the exact command and output.

### 5. Validate P9 Artifact Quality

```bash
cd /home/common/tmp/pycharm_Design
python analysis/07_p9_feature_quality_check.py
```

Expected output:

```text
overall_status: PASS
```

or:

```text
overall_status: PASS_WITH_WARNINGS
```

Acceptable warning:

- P9 feature table is CSV-derived and still requires explicit ODS/DWD/DWS parity decision.

Artifacts to inspect:

- `data/metropt_quality/analysis/reports/p9_feature_quality_report.md`
- `data/metropt_quality/analysis/models/p9_feature_quality_checks.json`

### 6. Regenerate P9 Features If Needed

```bash
cd /home/common/tmp/pycharm_Design
python analysis/05_p9_feature_engineering.py
python analysis/07_p9_feature_quality_check.py
```

Expected output:

- `p9_window_features_1min.parquet` exists and is non-empty.
- Feature row/column counts are recorded.
- Feature groups include minute statistics, 5/15/60-minute rolling windows, pressure deltas, digital activity, and state transitions.
- Label and RUL fields remain excluded from model features.

Master decision required:

- Keep P9 feature table as CSV-derived analysis artifact, or rebuild it from accepted ODS/DWS Parquet.

### 7. Rerun Baseline Models

```bash
cd /home/common/tmp/pycharm_Design
python analysis/06_p9_model_experiments.py
```

Expected checks:

- Target remains `pre_failure_24h`.
- Train/validation/test split remains chronological.
- Metrics include precision, recall, F1, PR-AUC, false alarms/day, lead time, and confusion matrix.
- `failure_window`, `pre_failure_*`, `post_maintenance`, `normal_candidate`, and `rul_seconds` do not enter model features.
- If scikit-learn is installed, Random Forest and Isolation Forest should train or produce explicit skip reasons.

Artifacts to inspect:

- `data/metropt_quality/analysis/reports/p9_model_baseline_report.md`
- `data/metropt_quality/analysis/models/p9_model_metrics.json`
- `data/metropt_quality/analysis/figures/p9_baseline_confusion_matrices.png`
- `data/metropt_quality/analysis/figures/p9_risk_score_timeline.png`

### 8. Validate Realtime Risk Design

Realtime-risk artifacts:

```text
streaming/metropt_realtime_risk_score_plan.py
data/metropt_quality/analysis/reports/p9_realtime_risk_design.md
data/metropt_quality/analysis/reports/p9_realtime_message_examples.md
```

Local contract smoke:

```bash
cd /home/common/tmp/pycharm_Design
python streaming/metropt_realtime_risk_score_plan.py --emit-examples valid --enrich
python streaming/metropt_realtime_risk_score_plan.py --emit-examples invalid
```

Expected checks:

- Valid examples contain `risk_score`, `risk_level`, `risk_model_name`, `risk_model_version`, `risk_threshold`, `feature_window_minutes`, and `feature_window_end`.
- Invalid examples produce explicit validation reasons.
- `dry_run_signal_proxy_not_production` remains documented as a contract-only score.

If master decides to validate existing realtime demo:

```bash
cd /home/common/tmp/pycharm_Design
bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500
```

Do not claim Kafka/Flink/Redis supports P9 online risk scoring until a real integration is implemented and logged.

### 9. Validate Hive BI Views

```bash
cd /home/common/tmp/pycharm_Design
python src/06_metropt_hive_views.py
```

Then run the Hive samples in `p9_dashboard_field_dictionary.md`.

Expected checks:

- `metropt_quality.vw_pbi_metropt_window_kpi` returns offline window KPI rows.
- `metropt_quality.vw_pbi_metropt_sensor_kpi` returns 15 sensor KPI rows.
- `metropt_quality.vw_pbi_metropt_realtime_kpi_1m` may be empty outside a realtime demo; emptiness alone is not a failure.

### 10. Validate Trino and Doris Samples If Extended Query Mode Is Enabled

Trino:

```bash
cd /home/common/tmp/pycharm_Design
bin/start_extended_query_mode.sh --trino-only
bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke
```

Doris:

```bash
cd /home/common/tmp/pycharm_Design
bin/start_extended_query_mode.sh --doris-only --allow-swapoff
bin/p5_doris_acceptance.sh --check-only
```

Expected checks:

- Query logs record return code and sample output.
- Do not mark P9 BI samples as passed until the actual SQL from `p9_dashboard_field_dictionary.md` is run or accepted as equivalent to existing smoke coverage.

### 11. Decide P9 Delivery Packaging

Cluster validation should decide whether to include P9 artifacts in a future formal delivery package.

Candidate include paths:

- `data/metropt_quality/analysis/reports/p9_*.md`
- `data/metropt_quality/analysis/models/p9_model_metrics.json`
- `data/metropt_quality/analysis/models/p9_feature_dictionary.tsv`
- `data/metropt_quality/analysis/models/p9_window_features_1min_sample.tsv`
- `data/metropt_quality/analysis/figures/p9_*.png`
- `streaming/metropt_realtime_risk_score_plan.py`
- `data/metropt_quality/analysis/reports/p9_*.md`

Candidate exclude or regenerate:

- `data/metropt_quality/analysis/models/p9_window_features_1min.parquet`, because it is large and reproducible.

## Final Master Verdict Format

Cluster validation should record:

| Item | Verdict | Evidence path | Notes |
| --- | --- | --- | --- |
| P9 docs linked | PASS / FAIL / SKIP |  |  |
| P9 artifact QA | PASS / WARN / FAIL / SKIP |  |  |
| P9 feature parity | PASS / WARN / FAIL / SKIP |  |  |
| P9 model rerun | PASS / WARN / FAIL / SKIP |  |  |
| P9 realtime risk design | PASS / WARN / FAIL / SKIP |  |  |
| Hive BI samples | PASS / WARN / FAIL / SKIP |  |  |
| Trino/Doris BI samples | PASS / WARN / FAIL / SKIP |  |  |
| Packaging decision | INCLUDE / EXCLUDE / DEFER |  |  |

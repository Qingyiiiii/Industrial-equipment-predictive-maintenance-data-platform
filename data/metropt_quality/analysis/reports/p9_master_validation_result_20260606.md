# P9 Cluster Validation Result

## Validation Conclusion

Validation owner: 验证负责人

Validation date: 2026-06-06

Master host project root: `/home/common/tmp/pycharm_Design`

Overall result: **PASS_WITH_BOUNDARIES**

P9 evidence can be accepted as an expansion-stage analysis, BI documentation, feature-quality, model-baseline, and realtime-risk contract package. The cluster-side base services, Hive BI views, dashboard sample SQL, and existing realtime demo were revalidated on the cluster.

Boundaries:

- P9 feature/model artifacts remain CSV-derived analysis artifacts. They have not yet been rebuilt from accepted ODS/DWD/DWS Parquet as a promoted production feature table.
- P9 realtime-risk scoring is contract/dry-run scope. The existing Kafka/Flink/Redis/Hive demo passed, but P9 online risk scoring has not been implemented inside the streaming job.
- Trino and Doris were not started in this validation round. Their P9 query samples remain SKIP / pending extended-query validation.

## Evidence Summary

| Area | Result | Evidence |
| --- | --- | --- |
| P9 evidence presence | PASS | Remote project contains the current `data/metropt_quality/analysis/reports/p9_*.md` report set and realtime-risk docs/script. |
| Initial P7 before startup | WARN context only | `p7_ops_snapshot_20260606_063716`: `pass=22 warn=19 skip=17 fail=0`; Hive/Kafka/Flink were not running before startup. |
| Base service startup | PASS | `data/metropt_quality/validation_runs/start_base_services_20260606_064545/summary.tsv`: `pass=12 warn=0 skip=0 fail=0`. |
| P7 after startup | PASS with Trino/Doris SKIP | `data/metropt_quality/validation_runs/p7_ops_snapshot_20260606_064808/`: `pass=41 warn=0 skip=17 fail=0`. Offline Hive/Spark and realtime demo readiness are PASS. |
| Python dependencies | PASS | `pandas=OK`, `pyarrow=OK`, `sklearn=OK`. |
| Python syntax | PASS | `analysis/` and `streaming/` syntax check passed for 14 files on remote. |
| P9 feature regeneration | PASS | `analysis/05_p9_feature_engineering.py` regenerated feature parquet, sample TSV, dictionary, EDA JSON/report, and figures. |
| P9 model rerun | PASS | `analysis/06_p9_model_experiments.py` regenerated `p9_model_metrics.json`, prediction sample, feature weights, report, and model figures. |
| P9 quality check | PASS_WITH_WARNINGS | First run failed only because model figures were missing before model rerun. Rerun `20260606_master_remote_after_model` returned `overall_status: PASS_WITH_WARNINGS`; remaining warning is local ODS/DWD/DWS Parquet parity pending. |
| Realtime risk contract smoke | PASS | `streaming/metropt_realtime_risk_score_plan.py --emit-examples valid --enrich` and `--emit-examples invalid` returned rc=0. Valid samples include risk fields; invalid samples emit malformed/contract-negative examples. |
| Hive BI views | PASS | `data/metropt_quality/validation_runs/p9_master_validation_20260606_064808/06_metropt_hive_views.log`: Spark-on-YARN created and sampled `vw_pbi_metropt_window_kpi`, `vw_pbi_metropt_sensor_kpi`, `vw_pbi_metropt_realtime_kpi_1m`. |
| P9 dashboard Hive SQL | PASS | `data/metropt_quality/validation_runs/p9_master_validation_20260606_064808/p9_hive_dashboard_samples.log`: both dashboard sample queries returned rc=0; window query returned 100 rows and sensor query returned 15 rows. |
| Existing realtime demo | PASS | `data/metropt_quality/validation_runs/p6_realtime_demo_20260606_065021/summary.tsv`: `pass=7 warn=0 skip=0 fail=0`. |
| Trino/Doris P9 samples | SKIP | Trino/Doris were intentionally not started in this round; P7 marks query readiness as SKIP. |

## Commands Executed On Master

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh

python - <<'PY'
import importlib.util
for name in ["pandas", "pyarrow", "sklearn"]:
    print(f"{name}\t" + ("OK" if importlib.util.find_spec(name) else "MISSING"))
PY

python analysis/05_p9_feature_engineering.py
python analysis/07_p9_feature_quality_check.py --run-id 20260606_master_remote
python analysis/06_p9_model_experiments.py
python analysis/07_p9_feature_quality_check.py --run-id 20260606_master_remote_after_model

python streaming/metropt_realtime_risk_score_plan.py --emit-examples valid --enrich
python streaming/metropt_realtime_risk_score_plan.py --emit-examples invalid

bin/start_base_services.sh --hive-count
bin/p7_ops_snapshot.sh

export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
export JAVA_HOME=/export/server/jdk17
/export/server/spark/bin/spark-submit src/06_metropt_hive_views.py

export JAVA_HOME=/export/server/jdk8
/export/server/hive/bin/beeline \
  -u 'jdbc:hive2://hadoop1:10000/default' \
  -n common \
  --showHeader=true \
  --outputformat=tsv2 \
  -f data/metropt_quality/validation_runs/p9_master_validation_20260606_064808/p9_hive_dashboard_samples.sql

bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500
```

## Acceptance Decision

Accept the P9 package for expansion-stage use.

Do not promote the following claims yet:

- Do not claim the P9 feature table is ODS/DWD/DWS-derived until it is rebuilt from accepted warehouse outputs or an explicit parity exception is approved.
- Do not claim P9 realtime risk scoring is online in Flink until the scoring contract is integrated into the streaming job and revalidated.
- Do not claim Trino/Doris P9 BI samples passed until extended-query mode is started and the actual SQL is run with logged rc/output.

Recommended next step: decide whether P9 should remain a portfolio/analysis package or be promoted into a P10 productionization track for warehouse-derived features and online scoring integration.

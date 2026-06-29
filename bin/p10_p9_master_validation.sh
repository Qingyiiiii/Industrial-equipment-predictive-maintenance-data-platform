#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

PYTHON_EXEC="${PYTHON_EXEC:-python3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-/export/server/spark/bin/spark-submit}"
CONFIG=""
VALIDATION_MODE="standard"
START_BASE=1
RECLAIM_MEMORY=1
RUN_P7=1
RUN_FEATURE_REBUILD=1
RUN_MODEL_BASELINE=1
RUN_HIVE_SQL=1
RUN_REALTIME=1
RUN_QUERY_LAYER=1
ALLOW_SWAPOFF=0
REALTIME_DURATION_MINUTES=0
REALTIME_MAX_EVENTS=1000
REALTIME_RATE=500
REALTIME_WAIT_SECONDS=60
QUERY_TIMEOUT=300
REALTIME_MAX_EVENTS_SET=0
REALTIME_WAIT_SECONDS_SET=0
QUERY_TIMEOUT_SET=0
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p10_p9_master_validation.sh [options]

Purpose:
  Run a repeatable P9/P10/P11/P12 master validation chain and generate
  summary.tsv, p14_steps.tsv, and validation_report.md in one run_dir.

Default chain:
  Python dependency check, static syntax check, P7 snapshot, base service startup,
  P10 warehouse feature rebuild, P10 feature quality check, P10 model baseline rerun,
  P9 Hive dashboard SQL, P6 realtime demo with P11 risk, and P12 Trino/Doris query layer.

Options:
  --mode MODE                 Validation mode: smoke, standard, or full. Default: standard.
                              smoke: dependencies, syntax, P7, base services, Hive samples.
                              standard: full P14 chain, no skipped sections for acceptance.
                              full: standard chain with larger realtime replay and query timeout.
  --config PATH                MetroPT cluster config. Default: config/metropt_quality.cluster.yaml.
  --skip-p7                    Skip P7 ops snapshot.
  --skip-base-start            Do not run bin/start_base_services.sh --hive-count.
  --skip-memory-reclaim        Do not stop restartable query/realtime services before model rerun.
  --skip-feature-rebuild       Skip P10 warehouse feature builder and quality check.
  --skip-model                 Skip P10 model baseline rerun.
  --skip-hive-sql              Skip P9 Hive dashboard SQL checks.
  --skip-realtime              Skip P6 realtime demo / P11 risk evidence.
  --skip-query-layer           Skip P12 Trino/Doris query-layer validation.
  --allow-swapoff              Pass --allow-swapoff to P12 Doris startup.
  --realtime-duration-minutes N
                               Extra P6 sampling duration after P1/P11 evidence, default: 0.
  --realtime-max-events N      Default: 1000.
  --realtime-rate N            Default: 500.
  --realtime-wait-seconds N    Default: 60.
  --query-timeout SECONDS      Default: 300.
  -h, --help                   Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      VALIDATION_MODE="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --skip-p7)
      RUN_P7=0
      shift
      ;;
    --skip-base-start)
      START_BASE=0
      shift
      ;;
    --skip-memory-reclaim)
      RECLAIM_MEMORY=0
      shift
      ;;
    --skip-feature-rebuild)
      RUN_FEATURE_REBUILD=0
      shift
      ;;
    --skip-model)
      RUN_MODEL_BASELINE=0
      shift
      ;;
    --skip-hive-sql)
      RUN_HIVE_SQL=0
      shift
      ;;
    --skip-realtime)
      RUN_REALTIME=0
      shift
      ;;
    --skip-query-layer)
      RUN_QUERY_LAYER=0
      shift
      ;;
    --allow-swapoff)
      ALLOW_SWAPOFF=1
      shift
      ;;
    --realtime-duration-minutes)
      REALTIME_DURATION_MINUTES="${2:-}"
      shift 2
      ;;
    --realtime-max-events)
      REALTIME_MAX_EVENTS="${2:-}"
      REALTIME_MAX_EVENTS_SET=1
      shift 2
      ;;
    --realtime-rate)
      REALTIME_RATE="${2:-}"
      shift 2
      ;;
    --realtime-wait-seconds)
      REALTIME_WAIT_SECONDS="${2:-}"
      REALTIME_WAIT_SECONDS_SET=1
      shift 2
      ;;
    --query-timeout)
      QUERY_TIMEOUT="${2:-}"
      QUERY_TIMEOUT_SET=1
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$VALIDATION_MODE" in
  smoke)
    RECLAIM_MEMORY=0
    RUN_FEATURE_REBUILD=0
    RUN_MODEL_BASELINE=0
    RUN_REALTIME=0
    RUN_QUERY_LAYER=0
    ;;
  standard)
    ;;
  full|nightly)
    VALIDATION_MODE="full"
    if ((REALTIME_MAX_EVENTS_SET == 0)); then
      REALTIME_MAX_EVENTS=10000
    fi
    if ((REALTIME_WAIT_SECONDS_SET == 0)); then
      REALTIME_WAIT_SECONDS=120
    fi
    if ((QUERY_TIMEOUT_SET == 0)); then
      QUERY_TIMEOUT=600
    fi
    ;;
  *)
    echo "--mode must be one of: smoke, standard, full" >&2
    exit 2
    ;;
esac

case "$REALTIME_DURATION_MINUTES:$REALTIME_MAX_EVENTS:$REALTIME_RATE:$REALTIME_WAIT_SECONDS:$QUERY_TIMEOUT" in
  *[!0-9:]*|"")
    echo "numeric arguments must be non-negative integers" >&2
    exit 2
    ;;
esac

p2_init "p14_master_validation" "${ORIGINAL_ARGS[@]}"
CONFIG="${CONFIG:-$P2_PROJECT_ROOT/config/metropt_quality.cluster.yaml}"
p2_header "P14 MetroPT P9-P12 master validation automation"
printf 'mode=%s\nconfig=%s\npython=%s\nstart_base=%s\nrun_p7=%s\nfeature_rebuild=%s\nmodel_baseline=%s\nhive_sql=%s\nrealtime=%s\nquery_layer=%s\nallow_swapoff=%s\nrealtime_duration_minutes=%s\nrealtime_max_events=%s\nrealtime_rate=%s\nrealtime_wait_seconds=%s\nquery_timeout=%s\n\n' \
  "$VALIDATION_MODE" "$CONFIG" "$PYTHON_EXEC" "$START_BASE" "$RUN_P7" "$RUN_FEATURE_REBUILD" "$RUN_MODEL_BASELINE" "$RUN_HIVE_SQL" "$RUN_REALTIME" "$RUN_QUERY_LAYER" "$ALLOW_SWAPOFF" "$REALTIME_DURATION_MINUTES" "$REALTIME_MAX_EVENTS" "$REALTIME_RATE" "$REALTIME_WAIT_SECONDS" "$QUERY_TIMEOUT"
printf 'spark_submit=%s\nmemory_reclaim=%s\n\n' "$SPARK_SUBMIT" "$RECLAIM_MEMORY"

P14_STEPS="$P2_RUN_DIR/p14_steps.tsv"
P14_HIVE_RESULTS="$P2_RUN_DIR/p14_hive_results.tsv"
P14_REPORT="$P2_RUN_DIR/validation_report.md"
P14_SQL_DIR="$P2_RUN_DIR/sql"
mkdir -p "$P14_SQL_DIR"
printf 'step\tcomponent\tstatus\treturn_code\tchild_pass\tchild_warn\tchild_skip\tchild_fail\tlog\tchild_run_dir\tnext_action\tdetail\n' > "$P14_STEPS"
printf 'query_id\treturn_code\trows\tsql_file\toutput_file\tlog\n' > "$P14_HIVE_RESULTS"

extract_child_run_dir() {
  local log="$1"
  grep -E '^run_dir=' "$log" 2>/dev/null | tail -n 1 | sed 's/^run_dir=//' || true
}

extract_summary_value() {
  local log="$1"
  local key="$2"
  local line
  line="$(grep -E 'SUMMARY pass=' "$log" 2>/dev/null | tail -n 1 || true)"
  if [[ -z "$line" ]]; then
    echo ""
    return
  fi
  sed -n "s/.*$key=\([0-9][0-9]*\).*/\1/p" <<< "$line"
}

p14_record_step() {
  local step="$1"
  local component="$2"
  local status="$3"
  local rc="$4"
  local child_pass="$5"
  local child_warn="$6"
  local child_skip="$7"
  local child_fail="$8"
  local log="$9"
  local child_run_dir="${10}"
  local next_action="${11}"
  local detail="${12}"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$step" "$component" "$status" "$rc" "${child_pass:-}" "${child_warn:-}" "${child_skip:-}" "${child_fail:-}" \
    "$log" "${child_run_dir:-}" "$next_action" "$detail" >> "$P14_STEPS"
  p2_report "$status" "$step" "rc=$rc log=$log child_run_dir=${child_run_dir:-N/A} ${detail:-}"
  if [[ "$status" == "FAIL" ]]; then
    p2_failure_template "$step" "$component" "$rc" "$log" "" "$detail" "$next_action"
  fi
}

run_step() {
  local step="$1"
  local component="$2"
  local warn_on_child_warn="$3"
  local next_action="$4"
  shift 4
  local log="$P2_RUN_DIR/${step}.log"
  local start end rc child_run_dir child_pass child_warn child_skip child_fail status detail
  mkdir -p "$(dirname "$log")"
  {
    printf 'step=%s\n' "$step"
    printf 'component=%s\n' "$component"
    printf 'start=%s\n' "$(date '+%F %T')"
    printf 'command='
    printf '%q ' "$@"
    printf '\n\n'
  } > "$log"
  start="$(date +%s)"
  "$@" >> "$log" 2>&1
  rc=$?
  end="$(date +%s)"
  {
    printf '\nend=%s\n' "$(date '+%F %T')"
    printf 'return_code=%s\n' "$rc"
    printf 'real_seconds=%s\n' "$((end - start))"
  } >> "$log"

  child_run_dir="$(extract_child_run_dir "$log")"
  child_pass="$(extract_summary_value "$log" pass)"
  child_warn="$(extract_summary_value "$log" warn)"
  child_skip="$(extract_summary_value "$log" skip)"
  child_fail="$(extract_summary_value "$log" fail)"
  child_pass="${child_pass:-}"
  child_warn="${child_warn:-0}"
  child_skip="${child_skip:-}"
  child_fail="${child_fail:-0}"

  status="PASS"
  detail="seconds=$((end - start))"
  if ((rc != 0)) || [[ "${child_fail:-0}" =~ ^[0-9]+$ && "${child_fail:-0}" -gt 0 ]]; then
    status="FAIL"
    detail="command failed or child summary has fail=${child_fail:-UNKNOWN}; seconds=$((end - start))"
  elif ((warn_on_child_warn == 1)) && [[ "${child_warn:-0}" =~ ^[0-9]+$ && "${child_warn:-0}" -gt 0 ]]; then
    status="WARN"
    detail="child summary has warn=$child_warn; seconds=$((end - start))"
  fi

  p14_record_step "$step" "$component" "$status" "$rc" "$child_pass" "$child_warn" "$child_skip" "$child_fail" "$log" "$child_run_dir" "$next_action" "$detail"
  return "$rc"
}

skip_step() {
  local step="$1"
  local component="$2"
  local reason="$3"
  local next_action="$4"
  p14_record_step "$step" "$component" "SKIP" "NA" "" "" "" "" "" "" "$next_action" "$reason"
}

check_python_dependencies() {
  local spark_submit_ok=0
  if [[ -x "$SPARK_SUBMIT" ]] || command -v spark-submit >/dev/null 2>&1; then
    spark_submit_ok=1
    printf 'spark_submit\tOK\t%s\n' "$SPARK_SUBMIT"
  else
    printf 'spark_submit\tMISSING\t%s\n' "$SPARK_SUBMIT"
  fi
  P14_SPARK_SUBMIT_OK="$spark_submit_ok" \
  "$PYTHON_EXEC" - <<'PY'
import importlib.util
import os
import sys

modules = ["pandas", "pyarrow", "sklearn", "numpy"]
missing = []
for name in modules:
    ok = importlib.util.find_spec(name) is not None
    print(f"{name}\t{'OK' if ok else 'MISSING'}")
    if not ok:
        missing.append(name)
pyspark_ok = importlib.util.find_spec("pyspark") is not None
spark_submit_ok = os.environ.get("P14_SPARK_SUBMIT_OK") == "1"
print(f"pyspark\t{'OK' if pyspark_ok else 'MISSING'}")
if not pyspark_ok and not spark_submit_ok:
    missing.append("pyspark_or_spark_submit")
if missing:
    print("missing_required=" + ",".join(missing))
    sys.exit(1)
PY
}

check_static_syntax() {
  "$PYTHON_EXEC" - <<'PY'
import ast
import sys
from pathlib import Path

roots = [Path("analysis"), Path("streaming")]
checked = 0
errors = []
for root in roots:
    for path in root.rglob("*.py"):
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            checked += 1
        except Exception as exc:
            errors.append(f"{path}: {exc}")
for item in errors:
    print(item)
print(f"checked_python_files={checked}")
if errors:
    sys.exit(1)
PY
}

check_p10_parity_json() {
  "$PYTHON_EXEC" - <<'PY'
import json
import sys
from pathlib import Path

path = Path("data/metropt_quality/analysis/models/p10_feature_parity_summary.json")
if not path.exists():
    print(f"missing={path}")
    sys.exit(1)
payload = json.loads(path.read_text(encoding="utf-8"))
status = payload.get("overall_status")
rows = payload.get("warehouse_feature", {}).get("row_count")
cols = payload.get("warehouse_feature", {}).get("column_count")
print(f"overall_status={status}")
print(f"warehouse_rows={rows}")
print(f"warehouse_columns={cols}")
sys.exit(0 if status in {"PASS", "PASS_WITH_WARNINGS"} and rows and cols else 1)
PY
}

check_p10_quality_json() {
  "$PYTHON_EXEC" - <<'PY'
import json
import sys
from pathlib import Path

path = Path("data/metropt_quality/analysis/models/p10_warehouse_feature_quality_checks.json")
if not path.exists():
    print(f"missing={path}")
    sys.exit(1)
payload = json.loads(path.read_text(encoding="utf-8"))
status = payload.get("overall_status")
print(f"overall_status={status}")
for row in payload.get("checks", []):
    print(f"{row.get('status')}\t{row.get('name')}\t{row.get('detail')}")
sys.exit(0 if status in {"PASS", "PASS_WITH_WARNINGS"} else 1)
PY
}

check_p10_model_outputs() {
  "$PYTHON_EXEC" - <<'PY'
import csv
import json
import sys
from pathlib import Path

json_path = Path("data/metropt_quality/analysis/models/p10_model_metric_comparison.json")
tsv_path = Path("data/metropt_quality/analysis/models/p10_model_metric_comparison.tsv")
if not json_path.exists() or not tsv_path.exists():
    print(f"missing json_or_tsv: {json_path} {tsv_path}")
    sys.exit(1)
payload = json.loads(json_path.read_text(encoding="utf-8"))
status = payload.get("status")
print(f"comparison_status={status}")
rows = list(csv.DictReader(tsv_path.open(encoding="utf-8"), delimiter="\t"))
warehouse = [row for row in rows if row.get("source_type") == "warehouse_derived"]
print(f"warehouse_model_rows={len(warehouse)}")
errors = []
for model in ["numpy_logistic_regression", "random_forest", "isolation_forest", "robust_anomaly_score"]:
    found = [row for row in warehouse if row.get("model_name") == model]
    if not found:
        errors.append(f"missing_model={model}")
        continue
    row = found[0]
    print(f"{model}\t{row.get('status')}\tprecision={row.get('precision')}\trecall={row.get('recall')}\tf1={row.get('f1')}\tpr_auc={row.get('pr_auc')}")
    if row.get("status") == "skipped" and not row.get("reason"):
        errors.append(f"skipped_without_reason={model}")
lead_rows = [row for row in warehouse if row.get("lead_time_model")]
if any(row.get("lead_time_model") != "numpy_logistic_regression" for row in lead_rows):
    errors.append("lead_time_assigned_to_non_logistic_model")
for item in errors:
    print("ERROR", item)
sys.exit(0 if status == "PASS" and not errors else 1)
PY
}

run_p10_feature_builder() {
  (
    cd "$P2_PROJECT_ROOT" || exit 1
    export METROPT_CONFIG="$CONFIG"
    export JAVA_HOME=/export/server/jdk17
    export SPARK_HOME=/export/server/spark
    export PYSPARK_PYTHON=/usr/bin/python3
    export PATH=/usr/local/bin:/usr/bin:/bin:$JAVA_HOME/bin:$SPARK_HOME/bin:$PATH
    "$SPARK_SUBMIT" analysis/08_p10_warehouse_feature_builder.py
  )
}

reclaim_memory_before_model() {
  local host
  export JAVA_HOME=/export/server/jdk17
  /export/server/flink/bin/stop-cluster.sh 2>/dev/null || true
  for host in hadoop1 hadoop2 hadoop3; do
    p2_run_on "$host" "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-server-stop.sh 2>/dev/null || true"
    p2_run_on "$host" "export JAVA_HOME=/export/server/jdk25; /export/server/trino/bin/launcher stop 2>/dev/null || true"
    p2_run_on "$host" "/export/server/doris/be/bin/stop_be.sh 2>/dev/null || true"
  done
  /export/server/doris/fe/bin/stop_fe.sh 2>/dev/null || true
  sleep 5
  printf 'memory_reclaim_completed=1\n'
}

run_p10_model_baseline() {
  (
    cd "$P2_PROJECT_ROOT" || exit 1
    export METROPT_CONFIG="$CONFIG"
    export P9_SKLEARN_N_JOBS=1
    export OMP_NUM_THREADS=1
    export OPENBLAS_NUM_THREADS=1
    export MKL_NUM_THREADS=1
    export NUMEXPR_NUM_THREADS=1
    "$PYTHON_EXEC" analysis/10_p10_warehouse_model_baseline.py
  )
}

ensure_realtime_services_ready() {
  local attempt log
  for attempt in 1 2 3; do
    log="$P2_RUN_DIR/ensure_realtime_services_attempt_${attempt}.log"
    {
      printf 'attempt=%s\nstart=%s\n' "$attempt" "$(date '+%F %T')"
      cd "$P2_PROJECT_ROOT" || return 1
      bin/start_base_services.sh --hive-count
      base_rc=$?
      printf '\nbase_rc=%s\n' "$base_rc"
      bin/p0_cluster_health_check.sh --module kafka
      kafka_rc=$?
      bin/p0_cluster_health_check.sh --module redis-flink
      redis_flink_rc=$?
      bin/p0_cluster_health_check.sh --module hive --skip-hive-count
      hive_rc=$?
      printf 'kafka_rc=%s\nredis_flink_rc=%s\nhive_rc=%s\n' "$kafka_rc" "$redis_flink_rc" "$hive_rc"
    } > "$log" 2>&1
    if [[ "${kafka_rc:-1}" -eq 0 && "${redis_flink_rc:-1}" -eq 0 && "${hive_rc:-1}" -eq 0 ]]; then
      printf 'realtime_services_ready=1 attempt=%s log=%s\n' "$attempt" "$log"
      return 0
    fi
    printf 'realtime_services_ready=0 attempt=%s log=%s\n' "$attempt" "$log"
    sleep 20
  done
  return 1
}

write_hive_sql_files() {
  cat > "$P14_SQL_DIR/hive_01_p9_window_dashboard.sql" <<SQL
USE metropt_quality;
SELECT
  dt,
  operating_state,
  COUNT(*) AS minute_count,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count,
  AVG(failure_window_rate) AS avg_failure_window_rate,
  AVG(avg_oil_temperature) AS avg_oil_temperature,
  AVG(avg_motor_current) AS avg_motor_current
FROM vw_pbi_metropt_window_kpi
GROUP BY dt, operating_state
ORDER BY dt, operating_state
LIMIT 100;
SQL
  cat > "$P14_SQL_DIR/hive_02_p9_sensor_dashboard.sql" <<SQL
USE metropt_quality;
SELECT
  sensor_name,
  sensor_type,
  unit,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM vw_pbi_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;
SQL
}

rows_from_output() {
  local output="$1"
  local lines
  if [[ ! -s "$output" ]]; then
    echo 0
    return
  fi
  lines="$(wc -l < "$output" | tr -d ' ')"
  if [[ "$lines" =~ ^[0-9]+$ && "$lines" -gt 0 ]]; then
    echo $((lines - 1))
  else
    echo 0
  fi
}

run_hive_query() {
  local query_id="$1"
  local sql_file="$2"
  local min_rows="$3"
  local output="$P2_RUN_DIR/${query_id}.tsv"
  export JAVA_HOME=/export/server/jdk8
  /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=true --outputformat=tsv2 -f "$sql_file" > "$output"
  local rc=$?
  local rows
  rows="$(rows_from_output "$output")"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$query_id" "$rc" "$rows" "$sql_file" "$output" "$P2_RUN_DIR/${query_id}.log" >> "$P14_HIVE_RESULTS"
  printf 'output=%s\nrows=%s\n' "$output" "$rows"
  if ((rc != 0)); then
    return "$rc"
  fi
  if [[ "$rows" =~ ^[0-9]+$ && "$rows" -ge "$min_rows" ]]; then
    return 0
  fi
  printf 'row count below threshold: rows=%s min_rows=%s\n' "$rows" "$min_rows"
  return 1
}

write_validation_report() {
  local final_status="PASS"
  if ((P2_FAIL_COUNT > 0)); then
    final_status="FAIL"
  elif ((P2_WARN_COUNT > 0 && P2_SKIP_COUNT > 0)); then
    final_status="PASS_WITH_WARNINGS_AND_SKIPS"
  elif ((P2_WARN_COUNT > 0)); then
    final_status="PASS_WITH_WARNINGS"
  elif ((P2_SKIP_COUNT > 0)); then
    final_status="PASS_WITH_SKIPS"
  fi
  if [[ "$VALIDATION_MODE" != "standard" && "$final_status" == PASS* ]]; then
    case "$VALIDATION_MODE" in
      smoke) final_status="SMOKE_${final_status}" ;;
      full) final_status="FULL_${final_status}" ;;
    esac
  fi
  cat > "$P14_REPORT" <<EOF
# P14 Master Validation Report

- run_id: $P2_RUN_ID
- generated_at: $(date '+%F %T')
- project_root: $P2_PROJECT_ROOT
- run_dir: $P2_RUN_DIR
- final_status: $final_status
- config: $CONFIG

## Scope

This report is generated by \`bin/p10_p9_master_validation.sh\`. It automates the P9/P10/P11/P12 master validation path with fixed PASS/WARN/SKIP/FAIL rules and keeps one log path plus one next action for every step.

## Options

| Option | Value |
| --- | --- |
| mode | $VALIDATION_MODE |
| start_base | $START_BASE |
| memory_reclaim | $RECLAIM_MEMORY |
| run_p7 | $RUN_P7 |
| spark_submit | $SPARK_SUBMIT |
| feature_rebuild | $RUN_FEATURE_REBUILD |
| model_baseline | $RUN_MODEL_BASELINE |
| hive_sql | $RUN_HIVE_SQL |
| realtime | $RUN_REALTIME |
| query_layer | $RUN_QUERY_LAYER |
| allow_swapoff | $ALLOW_SWAPOFF |
| realtime_duration_minutes | $REALTIME_DURATION_MINUTES |
| realtime_max_events | $REALTIME_MAX_EVENTS |
| realtime_rate | $REALTIME_RATE |
| realtime_wait_seconds | $REALTIME_WAIT_SECONDS |
| query_timeout | $QUERY_TIMEOUT |

## Step Results

\`\`\`tsv
$(cat "$P14_STEPS")
\`\`\`

## Hive SQL Results

\`\`\`tsv
$(cat "$P14_HIVE_RESULTS")
\`\`\`

## Rule Boundary

- Non-zero command return code is FAIL.
- Child validation summary with fail > 0 is FAIL.
- P7/base/realtime/query child warnings are WARN when no child failure exists.
- P10 feature quality accepts only PASS or PASS_WITH_WARNINGS.
- P10 model comparison accepts only status PASS and requires skipped RF/IF entries to have reasons.
- Hive dashboard SQL requires rc=0 and non-empty sample output.
- Disabled optional sections are SKIP and must be visible in \`p14_steps.tsv\`.

## Failure Handling

Every FAIL row in \`p14_steps.tsv\` contains:

- primary log path
- child run_dir when available
- next_action command
- detail

Do not manually convert FAIL to PASS without rerunning the failed step or documenting a replacement evidence path.
EOF
}

run_step "python_dependency_check" "Python" 0 \
  "Install missing Python packages in the active project runtime, then rerun this script." \
  check_python_dependencies

run_step "static_python_syntax_check" "Python" 0 \
  "Fix the reported Python syntax error before rerunning validation." \
  check_static_syntax

if ((RUN_P7 == 1)); then
  run_step "p7_ops_snapshot" "P7" 1 \
    "Inspect the P7 run_dir readiness.tsv and start missing services before deeper validation." \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p7_ops_snapshot.sh"
else
  skip_step "p7_ops_snapshot" "P7" "disabled by --skip-p7" "Rerun without --skip-p7 for standard acceptance."
fi

if ((START_BASE == 1)); then
  run_step "start_base_services" "P0/P2" 1 \
    "Run bin/start_base_services.sh --hive-count manually and inspect the child run_dir logs." \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_base_services.sh --hive-count"
else
  skip_step "start_base_services" "P0/P2" "disabled by --skip-base-start" "Rerun without --skip-base-start on a restarted machine."
fi

if ((RUN_FEATURE_REBUILD == 1)); then
  if ((RECLAIM_MEMORY == 1)); then
    run_step "reclaim_memory_before_feature_builder" "P14" 0 \
      "If this step fails, stop Flink/Kafka/Trino/Doris manually or rerun after reboot before the feature builder." \
      reclaim_memory_before_model
  else
    skip_step "reclaim_memory_before_feature_builder" "P14" "disabled by --skip-memory-reclaim" "Rerun without --skip-memory-reclaim if feature building is killed."
  fi
  run_step "p10_warehouse_feature_builder" "P10" 0 \
    "Check HDFS/YARN/Spark logs, then rerun METROPT_CONFIG=$CONFIG $SPARK_SUBMIT analysis/08_p10_warehouse_feature_builder.py." \
    run_p10_feature_builder
  run_step "p10_feature_parity_status" "P10" 0 \
    "Open data/metropt_quality/analysis/models/p10_feature_parity_summary.json and fix any FAIL verdict before rerunning." \
    check_p10_parity_json
  run_step "p10_warehouse_feature_quality" "P10" 0 \
    "Rerun '$PYTHON_EXEC' analysis/09_p10_warehouse_feature_quality_check.py and inspect p10_warehouse_feature_quality_report.md." \
    bash -lc "cd '$P2_PROJECT_ROOT' && METROPT_CONFIG='$CONFIG' '$PYTHON_EXEC' analysis/09_p10_warehouse_feature_quality_check.py"
  run_step "p10_feature_quality_status" "P10" 0 \
    "Fix feature-quality FAIL rows in p10_warehouse_feature_quality_checks.json, then rerun P10 quality." \
    check_p10_quality_json
else
  skip_step "reclaim_memory_before_feature_builder" "P14" "disabled by --skip-feature-rebuild" "Rerun without --skip-feature-rebuild for standard acceptance."
  skip_step "p10_warehouse_feature_builder" "P10" "disabled by --skip-feature-rebuild" "Rerun without --skip-feature-rebuild for standard acceptance."
  skip_step "p10_feature_quality_status" "P10" "disabled by --skip-feature-rebuild" "Rerun without --skip-feature-rebuild for standard acceptance."
fi

if ((RUN_MODEL_BASELINE == 1)); then
  if ((RECLAIM_MEMORY == 1)); then
    run_step "reclaim_memory_before_model" "P14" 0 \
      "If this step fails, stop Flink/Kafka/Trino/Doris manually or rerun after reboot before the model baseline." \
      reclaim_memory_before_model
  else
    skip_step "reclaim_memory_before_model" "P14" "disabled by --skip-memory-reclaim" "Rerun without --skip-memory-reclaim if model training is killed."
  fi
  run_step "p10_warehouse_model_baseline" "P10" 0 \
    "Inspect model logs and rerun '$PYTHON_EXEC' analysis/10_p10_warehouse_model_baseline.py after fixing dependencies or feature inputs." \
    run_p10_model_baseline
  run_step "p10_model_output_status" "P10" 0 \
    "Fix comparison status, leakage, split, or skipped model reason gaps in p10_model_metric_comparison.*." \
    check_p10_model_outputs
else
  skip_step "p10_warehouse_model_baseline" "P10" "disabled by --skip-model" "Rerun without --skip-model for standard acceptance."
  skip_step "p10_model_output_status" "P10" "disabled by --skip-model" "Rerun without --skip-model for standard acceptance."
fi

if ((RUN_HIVE_SQL == 1)); then
  write_hive_sql_files
  run_step "hive_p9_window_dashboard_sql" "Hive" 0 \
    "Check HiveServer2 and vw_pbi_metropt_window_kpi, then rerun the SQL file recorded in the log." \
    run_hive_query "hive_01_p9_window_dashboard" "$P14_SQL_DIR/hive_01_p9_window_dashboard.sql" 1
  run_step "hive_p9_sensor_dashboard_sql" "Hive" 0 \
    "Check HiveServer2 and vw_pbi_metropt_sensor_kpi, then rerun the SQL file recorded in the log." \
    run_hive_query "hive_02_p9_sensor_dashboard" "$P14_SQL_DIR/hive_02_p9_sensor_dashboard.sql" 1
else
  skip_step "hive_p9_window_dashboard_sql" "Hive" "disabled by --skip-hive-sql" "Rerun without --skip-hive-sql for standard acceptance."
  skip_step "hive_p9_sensor_dashboard_sql" "Hive" "disabled by --skip-hive-sql" "Rerun without --skip-hive-sql for standard acceptance."
fi

if ((RUN_REALTIME == 1)); then
  run_step "ensure_realtime_services_ready" "P11/P6" 1 \
    "Inspect ensure_realtime_services_attempt_*.log, then rerun bin/start_base_services.sh --hive-count until kafka/redis-flink/hive gates pass." \
    ensure_realtime_services_ready
  run_step "p6_realtime_demo_with_p11_risk" "P11/P6" 1 \
    "Inspect the child p6_realtime_demo run_dir, then run bin/start_realtime_mode.sh and rerun this step." \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p6_realtime_demo_mode.sh --start --duration-minutes '$REALTIME_DURATION_MINUTES' --max-events '$REALTIME_MAX_EVENTS' --rate '$REALTIME_RATE' --wait-seconds '$REALTIME_WAIT_SECONDS'"
else
  skip_step "ensure_realtime_services_ready" "P11/P6" "disabled by --skip-realtime" "Rerun without --skip-realtime for full P11/P6 evidence."
  skip_step "p6_realtime_demo_with_p11_risk" "P11/P6" "disabled by --skip-realtime" "Rerun without --skip-realtime for full P11/P6 evidence."
fi

if ((RUN_QUERY_LAYER == 1)); then
  p12_args=(--timeout "$QUERY_TIMEOUT")
  if ((ALLOW_SWAPOFF == 1)); then
    p12_args+=(--allow-swapoff)
  fi
  run_step "p12_trino_doris_query_layer" "P12" 1 \
    "Inspect the child p12_query_layer_validation run_dir; if Doris swap is the blocker, rerun with --allow-swapoff after approval." \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p12_query_layer_validation.sh ${p12_args[*]}"
else
  skip_step "p12_trino_doris_query_layer" "P12" "disabled by --skip-query-layer" "Rerun without --skip-query-layer for full Trino/Doris evidence."
fi

write_validation_report
p2_report PASS "p14_validation_report" "report=$P14_REPORT"
p2_finish

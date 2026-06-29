#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

CONFIG="/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml"
RUN_OFFLINE_FULL=0
RUN_REALTIME=0
SUBMIT_FLINK=0
MAX_EVENTS=10000
RATE=500
WAIT_SECONDS=90
QUERY_ENGINES="trino,doris"
SPARK_TIMEOUT=600

ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p3_project_delivery_acceptance.sh [options]

Default delivery-safe mode:
  - P0 config drift and base service check-only
  - P1 offline acceptance with --skip-run --skip-trino
  - P2 resource baseline all modes
  - P2 log cleanup dry-run
  - P3 data quality check
  - P2 query compare for trino,doris only, so inactive engines are SKIP

Options:
  --config PATH              MetroPT cluster config.
  --run-offline-full         Re-run P1 offline runner 00-06 instead of validating existing outputs.
  --run-realtime             Run P1 realtime acceptance.
  --submit-flink             With --run-realtime, submit a new Flink job. Default uses --skip-flink-submit.
  --max-events N             Realtime replay events, default: 10000.
  --rate N                   Realtime replay rate, default: 500.
  --wait-seconds N           Realtime wait seconds, default: 90.
  --query-engines LIST       Query compare engines, default: trino,doris.
  --spark-timeout SECONDS    P3 Spark data-quality timeout, default: 600.
  -h, --help                 Show help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --run-offline-full)
      RUN_OFFLINE_FULL=1
      shift
      ;;
    --run-realtime)
      RUN_REALTIME=1
      shift
      ;;
    --submit-flink)
      SUBMIT_FLINK=1
      shift
      ;;
    --max-events)
      MAX_EVENTS="${2:-}"
      shift 2
      ;;
    --rate)
      RATE="${2:-}"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:-}"
      shift 2
      ;;
    --query-engines)
      QUERY_ENGINES="${2:-}"
      shift 2
      ;;
    --spark-timeout)
      SPARK_TIMEOUT="${2:-}"
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

case "$MAX_EVENTS:$RATE:$WAIT_SECONDS:$SPARK_TIMEOUT" in
  *[!0-9:]*)
    echo "numeric arguments must be non-negative integers" >&2
    exit 2
    ;;
esac

p2_init "p3_project_delivery" "${ORIGINAL_ARGS[@]}"
p2_header "P3 MetroPT project delivery acceptance"
printf 'config=%s\nrun_offline_full=%s\nrun_realtime=%s\nsubmit_flink=%s\nquery_engines=%s\n\n' \
  "$CONFIG" "$RUN_OFFLINE_FULL" "$RUN_REALTIME" "$SUBMIT_FLINK" "$QUERY_ENGINES"

DELIVERY_SUMMARY="$P2_RUN_DIR/delivery_summary.md"
cat > "$DELIVERY_SUMMARY" <<EOF
# P3 MetroPT Project Delivery Acceptance

- run_id: $P2_RUN_ID
- started_at: $(date '+%F %T')
- project_root: $P2_PROJECT_ROOT
- run_dir: $P2_RUN_DIR
- mode: delivery-safe

## Steps

EOF

append_step() {
  local step="$1"
  local status="$2"
  local detail="$3"
  printf -- '- %s: %s - %s\n' "$status" "$step" "$detail" >> "$DELIVERY_SUMMARY"
}

run_step() {
  local step="$1"
  local component="$2"
  local log="$P2_RUN_DIR/${step}.log"
  local hint="$3"
  local next="$4"
  shift 4
  if p2_run_logged "$step" "$component" "$log" "$hint" "$next" "$@"; then
    append_step "$step" "PASS" "$log"
  else
    append_step "$step" "FAIL" "$log"
  fi
}

run_step "p0_config_drift" "P0" \
  "Config drift check failed; fix JDK/config pollution before delivery acceptance." \
  "cd $P2_PROJECT_ROOT && bin/p0_config_drift_check.sh" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_config_drift_check.sh"

run_step "base_services_check_only" "P2" \
  "Base service check failed; inspect P0 module logs." \
  "cd $P2_PROJECT_ROOT && bin/start_base_services.sh --check-only" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_base_services.sh --check-only"

offline_args=(--config "$CONFIG" --skip-trino)
if ((RUN_OFFLINE_FULL == 0)); then
  offline_args+=(--skip-run)
fi
run_step "p1_offline_delivery" "P1 offline" \
  "Offline delivery acceptance failed; inspect Spark/YARN and Hive logs." \
  "cd $P2_PROJECT_ROOT && bin/p1_metropt_offline_acceptance.sh --skip-run --skip-trino" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p1_metropt_offline_acceptance.sh ${offline_args[*]@Q}"

if ((RUN_REALTIME == 1)); then
  realtime_args=(--config "$CONFIG" --max-events "$MAX_EVENTS" --rate "$RATE" --wait-seconds "$WAIT_SECONDS")
  if ((SUBMIT_FLINK == 0)); then
    realtime_args+=(--skip-flink-submit)
  fi
  run_step "p1_realtime_delivery" "P1 realtime" \
    "Realtime delivery acceptance failed; inspect Kafka/Flink/Hive/Redis logs." \
    "cd $P2_PROJECT_ROOT && bin/p1_metropt_realtime_acceptance.sh --max-events $MAX_EVENTS --rate $RATE" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p1_metropt_realtime_acceptance.sh ${realtime_args[*]@Q}"
else
  run_step "realtime_mode_check_only" "P2 realtime" \
    "Realtime dependency check failed." \
    "cd $P2_PROJECT_ROOT && bin/start_realtime_mode.sh --check-only" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_realtime_mode.sh --check-only"
fi

run_step "p2_resource_baseline_all" "P2 resource" \
  "Resource baseline failed; inspect per-host snapshots." \
  "cd $P2_PROJECT_ROOT && bin/p2_resource_baseline.sh --mode all" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p2_resource_baseline.sh --mode all"

run_step "p2_log_maintenance_dry_run" "P2 logs" \
  "Log maintenance dry-run failed; inspect per-host scans." \
  "cd $P2_PROJECT_ROOT && bin/p2_log_maintenance_plan.sh --dry-run" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p2_log_maintenance_plan.sh --dry-run"

run_step "p3_data_quality" "P3 data quality" \
  "Data quality check failed; inspect Spark/Hive realtime snapshots." \
  "cd $P2_PROJECT_ROOT && bin/p3_data_quality_check.sh --config '$CONFIG'" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p3_data_quality_check.sh --config '$CONFIG' --spark-timeout '$SPARK_TIMEOUT'"

run_step "query_engine_positioning" "P2 query" \
  "Query engine positioning failed; inactive Trino/Doris should be SKIP, not FAIL." \
  "cd $P2_PROJECT_ROOT && bin/p2_query_perf_compare.sh --engine '$QUERY_ENGINES' --timeout 30" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p2_query_perf_compare.sh --engine '$QUERY_ENGINES' --timeout 30"

cat >> "$DELIVERY_SUMMARY" <<EOF

## Result

- pass: $P2_PASS_COUNT
- warn: $P2_WARN_COUNT
- skip: $P2_SKIP_COUNT
- fail: $P2_FAIL_COUNT
- finished_at: $(date '+%F %T')
- summary_tsv: $P2_SUMMARY

EOF

p2_report PASS "delivery_summary" "summary=$DELIVERY_SUMMARY"
p2_finish

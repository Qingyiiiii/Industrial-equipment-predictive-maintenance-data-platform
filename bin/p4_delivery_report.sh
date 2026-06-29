#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

P3_RUN=""
OUTPUT_ROOT=""
REPORT_NAME=""
REFRESH_STATUS=1
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p4_delivery_report.sh [options]

Options:
  --p3-run PATH          P3 project delivery run directory. Default: latest p3_project_delivery_*.
  --output-root PATH     Report output root. Default: data/metropt_quality/delivery_reports.
  --report-name NAME     Report directory name. Default: p4_delivery_<run_id>.
  --no-current-status    Do not probe Flink/Redis/Trino/Doris current status.
  -h, --help             Show help.

Creates:
  delivery_report.md
  delivery_summary.json
  current_status.tsv
  validation_run_index.tsv
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --p3-run)
      P3_RUN="${2:-}"
      shift 2
      ;;
    --output-root)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --report-name)
      REPORT_NAME="${2:-}"
      shift 2
      ;;
    --no-current-status)
      REFRESH_STATUS=0
      shift
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

p2_init "p4_delivery_report" "${ORIGINAL_ARGS[@]}"
p2_header "P4 MetroPT delivery report"

VALIDATION_ROOT="$P2_PROJECT_ROOT/data/metropt_quality/validation_runs"
OUTPUT_ROOT="${OUTPUT_ROOT:-$P2_PROJECT_ROOT/data/metropt_quality/delivery_reports}"
REPORT_NAME="${REPORT_NAME:-p4_delivery_${P2_RUN_ID}}"
REPORT_DIR="$OUTPUT_ROOT/$REPORT_NAME"
mkdir -p "$REPORT_DIR"

if [[ -z "$P3_RUN" ]]; then
  P3_RUN="$(ls -1dt "$VALIDATION_ROOT"/p3_project_delivery_* 2>/dev/null | head -n 1 || true)"
fi

if [[ -z "$P3_RUN" || ! -d "$P3_RUN" ]]; then
  p2_report FAIL "p3_run" "P3 run directory not found"
  p2_failure_template "p3_run" "P4" 1 "$P2_RUN_DIR/command.tsv" "" \
    "P4 report requires a completed P3 project delivery run." \
    "cd $P2_PROJECT_ROOT && bin/p3_project_delivery_acceptance.sh"
  p2_finish
fi

REPORT_MD="$REPORT_DIR/delivery_report.md"
REPORT_JSON="$REPORT_DIR/delivery_summary.json"
STATUS_TSV="$REPORT_DIR/current_status.tsv"
RUN_INDEX="$REPORT_DIR/validation_run_index.tsv"
LATEST_DEMO_RUN="$(ls -1dt "$VALIDATION_ROOT"/p6_realtime_demo_* 2>/dev/null | head -n 1 || true)"
LATEST_DEMO_STATUS=""
if [[ -n "$LATEST_DEMO_RUN" ]]; then
  LATEST_DEMO_STATUS="$LATEST_DEMO_RUN/demo_status.tsv"
fi
LATEST_OPS_RUN="$(ls -1dt "$VALIDATION_ROOT"/p7_ops_snapshot_* 2>/dev/null | head -n 1 || true)"
LATEST_OPS_READINESS=""
LATEST_OPS_HOST_METRICS=""
LATEST_OPS_SERVICE_STATUS=""
LATEST_OPS_MD=""
LATEST_OPS_JSON=""
if [[ -n "$LATEST_OPS_RUN" ]]; then
  LATEST_OPS_READINESS="$LATEST_OPS_RUN/readiness.tsv"
  LATEST_OPS_HOST_METRICS="$LATEST_OPS_RUN/host_metrics.tsv"
  LATEST_OPS_SERVICE_STATUS="$LATEST_OPS_RUN/service_status.tsv"
  LATEST_OPS_MD="$LATEST_OPS_RUN/ops_snapshot.md"
  LATEST_OPS_JSON="$LATEST_OPS_RUN/ops_snapshot.json"
fi

printf 'component\tstatus\tdetail\n' > "$STATUS_TSV"

status_line() {
  local component="$1"
  local status="$2"
  local detail="$3"
  printf '%s\t%s\t%s\n' "$component" "$status" "$detail" >> "$STATUS_TSV"
}

read_tsv_value() {
  local file="$1"
  local key="$2"
  awk -F'\t' -v key="$key" '$1==key {value=$2} END{print value}' "$file" 2>/dev/null || true
}

ops_readiness_summary() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    printf 'no P7 ops snapshot found'
    return
  fi
  awk -F'\t' '
    NR>1 {
      total++
      count[$2]++
    }
    END {
      printf "total=%d ready=%d warn=%d not_ready=%d not_running=%d",
        total+0, count["READY"]+0, count["WARN"]+0, count["NOT_READY"]+0, count["NOT_RUNNING"]+0
    }
  ' "$file"
}

capture_current_status() {
  if ((REFRESH_STATUS == 0)); then
    status_line "current_status" "SKIP" "disabled by --no-current-status"
    return
  fi

  local flink_log="$REPORT_DIR/flink_list.log"
  if export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list > "$flink_log" 2>&1; then
    if grep -q 'No running jobs' "$flink_log"; then
      local demo_overall demo_state demo_flink_state demo_sent demo_failed
      demo_overall="$(read_tsv_value "$LATEST_DEMO_STATUS" overall_status)"
      demo_state="$(read_tsv_value "$LATEST_DEMO_STATUS" current_state)"
      demo_flink_state="$(read_tsv_value "$LATEST_DEMO_STATUS" flink_job_observed_state)"
      demo_sent="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_sent)"
      demo_failed="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_failed)"
      if [[ "$demo_overall" == "PASS" ]]; then
        status_line "flink_jobs" "PASS" "current_state=not_running; latest P6 demo passed; demo_state=${demo_state:-N/A}; flink_state=${demo_flink_state:-N/A}; sent=${demo_sent:-N/A}; failed=${demo_failed:-N/A}; run=$LATEST_DEMO_RUN"
      else
        status_line "flink_jobs" "WARN" "no running jobs; realtime KPI refresh evidence is not current"
      fi
    else
      status_line "flink_jobs" "PASS" "running or scheduled jobs observed; log=$flink_log"
    fi
  else
    status_line "flink_jobs" "WARN" "flink list failed; log=$flink_log"
  fi

  if [[ -f "$LATEST_DEMO_STATUS" ]]; then
    local demo_overall demo_state demo_flink_state demo_sent demo_failed
    demo_overall="$(read_tsv_value "$LATEST_DEMO_STATUS" overall_status)"
    demo_state="$(read_tsv_value "$LATEST_DEMO_STATUS" current_state)"
    demo_flink_state="$(read_tsv_value "$LATEST_DEMO_STATUS" flink_job_observed_state)"
    demo_sent="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_sent)"
    demo_failed="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_failed)"
    if [[ "$demo_overall" == "PASS" ]]; then
      status_line "realtime_demo" "PASS" "latest P6 demo passed; current_state=${demo_state:-N/A}; flink_state=${demo_flink_state:-N/A}; sent=${demo_sent:-N/A}; failed=${demo_failed:-N/A}; run=$LATEST_DEMO_RUN"
    elif [[ -n "$demo_overall" ]]; then
      status_line "realtime_demo" "WARN" "latest P6 demo status=$demo_overall; current_state=${demo_state:-N/A}; run=$LATEST_DEMO_RUN"
    else
      status_line "realtime_demo" "WARN" "latest P6 demo status missing; run=$LATEST_DEMO_RUN"
    fi
  else
    status_line "realtime_demo" "SKIP" "no P6 realtime demo run found"
  fi

  local redis_log="$REPORT_DIR/redis_kpi_keys.log"
  if redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' 2>/dev/null | head -n 20 > "$redis_log"; then
    if [[ -s "$redis_log" ]]; then
      status_line "redis_kpi_keys" "PASS" "sample=$(wc -l < "$redis_log" | tr -d ' ') keys; log=$redis_log"
    else
      status_line "redis_kpi_keys" "WARN" "no metropt:kpi:1m:* keys; run realtime acceptance to refresh"
    fi
  else
    status_line "redis_kpi_keys" "WARN" "redis scan failed"
  fi

  if ss -lntp 2>/dev/null | grep -qE '[:.]8080[[:space:]]'; then
    status_line "trino" "PASS" "coordinator port 8080 listening"
  else
    status_line "trino" "SKIP" "not running; start with bin/start_extended_query_mode.sh --trino-only"
  fi

  if ss -lntp 2>/dev/null | grep -qE '[:.]9030[[:space:]]'; then
    local doris_log="$REPORT_DIR/doris_backends.log"
    if command -v mysql >/dev/null 2>&1 \
      && mysql -h 127.0.0.1 -P 9030 -uroot --batch --raw -e 'SHOW BACKENDS;' > "$doris_log" 2>&1; then
      local alive_backends
      alive_backends="$(awk -F'\t' 'NR==1 {for (i=1;i<=NF;i++) if (tolower($i)=="alive") alive=i} NR>1 && alive && tolower($alive)=="true" {ok++} END{print ok+0}' "$doris_log")"
      if [[ "$alive_backends" -ge 3 ]]; then
        status_line "doris" "PASS" "FE mysql port 9030 listening; alive_backends=$alive_backends; log=$doris_log"
      else
        status_line "doris" "WARN" "FE mysql port 9030 listening but alive_backends=$alive_backends; log=$doris_log"
      fi
    else
      status_line "doris" "PASS" "FE mysql port 9030 listening; backend query not available"
    fi
  else
    status_line "doris" "SKIP" "not running; keep as extended mode until Trino comparison is stable"
  fi

  if [[ -f "$LATEST_OPS_READINESS" ]]; then
    status_line "ops_snapshot" "PASS" "$(ops_readiness_summary "$LATEST_OPS_READINESS"); run=$LATEST_OPS_RUN"
  else
    status_line "ops_snapshot" "SKIP" "no P7 ops snapshot found; run bin/p7_ops_snapshot.sh"
  fi
}

capture_run_index() {
  printf 'mtime\tpath\tsummary\n' > "$RUN_INDEX"
  while IFS= read -r run; do
    [[ -z "$run" ]] && continue
    local summary
    summary="$(grep -hE '^SUMMARY ' "$run"/*.log "$run"/summary.tsv 2>/dev/null | tail -n 1 | tr '\t' ' ' || true)"
    printf '%s\t%s\t%s\n' "$(stat -c '%y' "$run" | cut -d'.' -f1)" "$run" "${summary:-N/A}" >> "$RUN_INDEX"
  done < <(ls -1dt "$VALIDATION_ROOT"/* 2>/dev/null | head -n 20)
}

extract_nested_run_dir() {
  local log="$1"
  grep -m1 '^run_dir=' "$log" 2>/dev/null | cut -d= -f2- || true
}

summary_from_command_tsv() {
  local command_tsv="$1"
  if [[ ! -f "$command_tsv" ]]; then
    return 0
  fi
  local pass warn skip fail rc
  pass="$(awk -F'\t' '$1=="pass"{print $2}' "$command_tsv" | tail -n 1)"
  warn="$(awk -F'\t' '$1=="warn"{print $2}' "$command_tsv" | tail -n 1)"
  skip="$(awk -F'\t' '$1=="skip"{print $2}' "$command_tsv" | tail -n 1)"
  fail="$(awk -F'\t' '$1=="fail"{print $2}' "$command_tsv" | tail -n 1)"
  rc="$(awk -F'\t' '$1=="return_code"{print $2}' "$command_tsv" | tail -n 1)"
  if [[ -n "$pass$warn$skip$fail" ]]; then
    printf 'SUMMARY pass=%s warn=%s skip=%s fail=%s rc=%s\n' \
      "${pass:-0}" "${warn:-0}" "${skip:-0}" "${fail:-0}" "${rc:-N/A}"
  fi
}

p3_summary="$P3_RUN/summary.tsv"
p3_delivery="$P3_RUN/delivery_summary.md"
p3_data_quality_log="$P3_RUN/p3_data_quality.log"
p3_resource_log="$P3_RUN/p2_resource_baseline_all.log"
p3_log_plan_log="$P3_RUN/p2_log_maintenance_dry_run.log"
p3_query_log="$P3_RUN/query_engine_positioning.log"

dq_run="$(extract_nested_run_dir "$p3_data_quality_log")"
dq_metrics="$dq_run/data_quality_metrics.tsv"
dq_summary="$dq_run/summary.tsv"

resource_run="$(extract_nested_run_dir "$p3_resource_log")"
log_plan_run="$(extract_nested_run_dir "$p3_log_plan_log")"
query_perf_run="$(ls -1dt "$VALIDATION_ROOT"/query_perf_compare_* 2>/dev/null | head -n 1 || true)"
query_perf_results=""
query_perf_summary=""
query_perf_status="SKIP"
query_perf_detail="no query performance compare run found"
query_perf_has_trino=0
if [[ -n "$query_perf_run" && -d "$query_perf_run" ]]; then
  query_perf_results="$query_perf_run/query_perf_results.tsv"
  query_perf_summary="$(summary_from_command_tsv "$query_perf_run/command.tsv")"
  if [[ -z "$query_perf_summary" ]]; then
    query_perf_summary="$(grep -hE '^SUMMARY ' "$query_perf_run"/*.log "$query_perf_run"/summary.tsv 2>/dev/null | tail -n 1 || true)"
  fi
  if [[ -f "$query_perf_results" ]]; then
    query_perf_failed="$(awk -F'\t' 'NR>1 && $3 != 0 {c++} END{print c+0}' "$query_perf_results")"
    query_perf_engines="$(awk -F'\t' 'NR>1 {seen[$1]=1} END{first=1; for (e in seen) {printf "%s%s", first?"":",", e; first=0}}' "$query_perf_results")"
    if awk -F'\t' 'NR>1 && $1=="trino" && $3==0 {found=1} END{exit found?0:1}' "$query_perf_results"; then
      query_perf_has_trino=1
    fi
    if [[ "$query_perf_failed" == "0" ]]; then
      query_perf_status="PASS"
      query_perf_detail="engines=${query_perf_engines:-N/A}; results=$query_perf_results"
    else
      query_perf_status="WARN"
      query_perf_detail="failed_queries=$query_perf_failed; results=$query_perf_results"
    fi
  else
    query_perf_status="WARN"
    query_perf_detail="results file missing: $query_perf_results"
  fi
fi
demo_summary_line="N/A"
if [[ -f "$LATEST_DEMO_STATUS" ]]; then
  demo_overall_for_report="$(read_tsv_value "$LATEST_DEMO_STATUS" overall_status)"
  demo_state_for_report="$(read_tsv_value "$LATEST_DEMO_STATUS" current_state)"
  demo_flink_state_for_report="$(read_tsv_value "$LATEST_DEMO_STATUS" flink_job_observed_state)"
  demo_sent_for_report="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_sent)"
  demo_failed_for_report="$(read_tsv_value "$LATEST_DEMO_STATUS" replay_failed)"
  demo_summary_line="overall=${demo_overall_for_report:-N/A} state=${demo_state_for_report:-N/A} flink_state=${demo_flink_state_for_report:-N/A} sent=${demo_sent_for_report:-N/A} failed=${demo_failed_for_report:-N/A} run=$LATEST_DEMO_RUN"
fi

capture_current_status
capture_run_index

p3_summary_line="$(summary_from_command_tsv "$P3_RUN/command.tsv")"
if [[ -z "$p3_summary_line" ]]; then
  p3_summary_line="$(grep -hE '^SUMMARY ' "$P3_RUN"/*.log 2>/dev/null | tail -n 1 || true)"
fi
resource_summary="$(grep -hE '^SUMMARY ' "$p3_resource_log" 2>/dev/null | tail -n 1 || true)"
log_plan_summary="$(grep -hE '^SUMMARY ' "$p3_log_plan_log" 2>/dev/null | tail -n 1 || true)"
query_summary="$(grep -hE '^SUMMARY ' "$p3_query_log" 2>/dev/null | tail -n 1 || true)"
dq_summary_line="$(grep -hE '^SUMMARY ' "$p3_data_quality_log" 2>/dev/null | tail -n 1 || true)"

status_warn_count="$(awk -F'\t' 'NR>1 && $2=="WARN"{c++} END{print c+0}' "$STATUS_TSV")"
status_skip_count="$(awk -F'\t' 'NR>1 && $2=="SKIP"{c++} END{print c+0}' "$STATUS_TSV")"
overall_status="PASS_WITH_ACTION_ITEMS"
if [[ "$status_warn_count" == "0" && "$status_skip_count" == "0" ]]; then
  overall_status="PASS"
fi

ACTION_ITEMS_TXT="$REPORT_DIR/action_items.txt"
: > "$ACTION_ITEMS_TXT"
add_action_item() {
  printf '%s\n' "$1" >> "$ACTION_ITEMS_TXT"
}
if awk -F'\t' '$1=="flink_jobs" && $2=="WARN" {found=1} END{exit found?0:1}' "$STATUS_TSV"; then
  add_action_item "If a live realtime demo is required, resubmit P3 realtime with Flink; the latest P3 realtime acceptance evidence is already archived."
fi
if [[ "$query_perf_status" != "PASS" || "$query_perf_has_trino" != "1" ]]; then
  add_action_item "Start Trino and run Hive/Trino smoke comparison."
fi
if awk -F'\t' '$1=="doris" && $2=="SKIP" {found=1} END{exit found?0:1}' "$STATUS_TSV"; then
  add_action_item "Keep Doris as optional extended mode; start and validate FE/BE only when Doris comparison is scheduled."
fi
if [[ ! -s "$ACTION_ITEMS_TXT" ]]; then
  add_action_item "No immediate P4 action items."
fi

cat > "$REPORT_MD" <<EOF
# MetroPT-3 P4 Delivery Report

## Executive Summary

- report_run_id: $P2_RUN_ID
- generated_at: $(date '+%F %T')
- overall_status: $overall_status
- source_p3_run: $P3_RUN
- report_dir: $REPORT_DIR

P4 turns P0/P1/P2/P3 validation evidence into a delivery-facing report. The current offline data quality, base delivery evidence, realtime acceptance evidence, and validated query comparison evidence are recorded below. Runtime action items only reflect components that are not currently running or evidence that needs live-demo freshness.

## Acceptance Evidence

| Area | Evidence | Status |
| --- | --- | --- |
| P3 delivery | ${p3_summary_line:-see $p3_summary} | PASS |
| Resource baseline | ${resource_summary:-see $p3_resource_log} | PASS |
| Log maintenance dry-run | ${log_plan_summary:-see $p3_log_plan_log} | PASS |
| Data quality | ${dq_summary_line:-see $p3_data_quality_log} | PASS |
| Query positioning | ${query_summary:-see $p3_query_log} | PASS/SKIP by engine state |
| Query performance compare | ${query_perf_summary:-$query_perf_detail} | $query_perf_status |
| Realtime demo | $demo_summary_line | ${demo_overall_for_report:-SKIP} |

## Data Quality Metrics

EOF

if [[ -f "$dq_metrics" ]]; then
  {
    printf '| Metric | Actual | Expected | Result |\n'
    printf '| --- | ---: | ---: | --- |\n'
    awk -F'\t' 'NR>1 {printf "| `%s` | %s | %s | %s |\n", $2, $3, $4, $1}' "$dq_metrics"
  } >> "$REPORT_MD"
else
  printf 'Data quality metrics not found: `%s`\n' "$dq_metrics" >> "$REPORT_MD"
fi

cat >> "$REPORT_MD" <<EOF

## Current Runtime Status

| Component | Status | Detail |
| --- | --- | --- |
EOF
awk -F'\t' 'NR>1 {printf "| `%s` | %s | %s |\n", $1, $2, $3}' "$STATUS_TSV" >> "$REPORT_MD"

cat >> "$REPORT_MD" <<EOF

## Current Resource And Service Snapshot

- Ops snapshot run: \`${LATEST_OPS_RUN:-N/A}\`
- Ops snapshot summary: $(ops_readiness_summary "$LATEST_OPS_READINESS")

### Mode Readiness

| Mode | Status | Reason | Next Command |
| --- | --- | --- | --- |
EOF
if [[ -f "$LATEST_OPS_READINESS" ]]; then
  awk -F'\t' 'NR>1 {for (i=1;i<=NF;i++) gsub(/\|/,"/",$i); printf "| `%s` | %s | %s | `%s` |\n", $1, $2, $3, $4}' "$LATEST_OPS_READINESS" >> "$REPORT_MD"
else
  printf '| `ops_snapshot` | SKIP | No P7 snapshot found | `bin/p7_ops_snapshot.sh` |\n' >> "$REPORT_MD"
fi

cat >> "$REPORT_MD" <<EOF

### Host Resource Headroom

| Host | Status | Mem Available MB | Mem Available % | Load1 | Cores | Root Used % | Export Used % |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
EOF
if [[ -f "$LATEST_OPS_HOST_METRICS" ]]; then
  awk -F'\t' 'NR>1 {printf "| `%s` | %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $4, $5, $6, $7, $8, $9}' "$LATEST_OPS_HOST_METRICS" >> "$REPORT_MD"
else
  printf '| `N/A` | SKIP | 0 | 0 | 0 | 0 | N/A | N/A |\n' >> "$REPORT_MD"
fi

cat >> "$REPORT_MD" <<EOF

### Key Service Checks

| Component | Status | Detail |
| --- | --- | --- |
EOF
if [[ -f "$LATEST_OPS_SERVICE_STATUS" ]]; then
  awk -F'\t' '
    NR>1 && ($1 ~ /^(hdfs_filesystem|yarn_running_nodes|yarn_running_apps|hive_metastore_port|hiveserver2_port|kafka_quorum|kafka_topics_cli|redis_ping|flink_jobs|trino_select1|doris_backends)$/) {
      detail=$3
      gsub(/\|/,"/",detail)
      printf "| `%s` | %s | %s |\n", $1, $2, detail
    }
  ' "$LATEST_OPS_SERVICE_STATUS" >> "$REPORT_MD"
else
  printf '| `ops_snapshot` | SKIP | No P7 service snapshot found |\n' >> "$REPORT_MD"
fi

cat >> "$REPORT_MD" <<EOF

## Evidence Paths

- P3 summary: \`$p3_summary\`
- P3 delivery summary: \`$p3_delivery\`
- Data quality run: \`${dq_run:-N/A}\`
- Resource baseline run: \`${resource_run:-N/A}\`
- Log maintenance run: \`${log_plan_run:-N/A}\`
- Query positioning log: \`$p3_query_log\`
- Query performance compare run: \`${query_perf_run:-N/A}\`
- Query performance compare results: \`${query_perf_results:-N/A}\`
- Realtime demo run: \`${LATEST_DEMO_RUN:-N/A}\`
- Realtime demo status: \`${LATEST_DEMO_STATUS:-N/A}\`
- Ops snapshot run: \`${LATEST_OPS_RUN:-N/A}\`
- Ops snapshot markdown: \`${LATEST_OPS_MD:-N/A}\`
- Ops snapshot json: \`${LATEST_OPS_JSON:-N/A}\`
- Validation run index: \`$RUN_INDEX\`

## Recommended Next Actions
EOF

action_no=1
while IFS= read -r item; do
  [[ -z "$item" ]] && continue
  printf '%s. %s\n\n' "$action_no" "$item" >> "$REPORT_MD"
  case "$item" in
    If\ a\ live\ realtime\ demo*)
      cat >> "$REPORT_MD" <<'EOF'
```bash
bin/p3_project_delivery_acceptance.sh --run-realtime --submit-flink --max-events 10000 --rate 500
```

EOF
      ;;
    Start\ Trino*)
      cat >> "$REPORT_MD" <<'EOF'
```bash
bin/start_extended_query_mode.sh --trino-only
bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke
```

EOF
      ;;
  esac
  ((action_no++))
done < "$ACTION_ITEMS_TXT"

python3 - "$REPORT_JSON" "$overall_status" "$P2_RUN_ID" "$P3_RUN" "$REPORT_DIR" "$STATUS_TSV" "$ACTION_ITEMS_TXT" <<'PY'
import json
import sys
from pathlib import Path

out, status, run_id, p3_run, report_dir, status_tsv, action_items_file = sys.argv[1:]
components = []
with open(status_tsv, "r", encoding="utf-8") as f:
    header = f.readline()
    for line in f:
        component, state, detail = line.rstrip("\n").split("\t", 2)
        components.append({"component": component, "status": state, "detail": detail})
action_items = [
    line.strip()
    for line in Path(action_items_file).read_text(encoding="utf-8").splitlines()
    if line.strip()
]

payload = {
    "phase": "P4",
    "run_id": run_id,
    "overall_status": status,
    "source_p3_run": p3_run,
    "report_dir": report_dir,
    "components": components,
    "action_items": action_items,
}
Path(out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

p2_report PASS "p4_delivery_report" "report=$REPORT_MD"
p2_report PASS "p4_delivery_summary_json" "summary=$REPORT_JSON"
if [[ "$status_warn_count" != "0" || "$status_skip_count" != "0" ]]; then
  p2_report WARN "p4_action_items" "runtime gaps warn=$status_warn_count skip=$status_skip_count"
fi

printf '\nreport=%s\nsummary_json=%s\nstatus_tsv=%s\nrun_index=%s\n' \
  "$REPORT_MD" "$REPORT_JSON" "$STATUS_TSV" "$RUN_INDEX"
p2_finish

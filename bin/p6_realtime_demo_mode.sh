#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

CONFIG="/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml"
MODE="status"
DURATION_MINUTES=5
MAX_EVENTS=10000
RATE=500
BATCH_SIZE=500
WAIT_SECONDS=60
SUBMIT_TIMEOUT=120
AUTO_STOP=1
WITH_RISK=1
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p6_realtime_demo_mode.sh --start [--duration-minutes N] [--max-events N] [--rate N] [--keep-running] [--skip-risk]
  bin/p6_realtime_demo_mode.sh --stop
  bin/p6_realtime_demo_mode.sh --status

Purpose:
  Run a bounded MetroPT realtime KPI + P11 risk demo, capture Flink/Redis/Hive evidence, and keep P4 reports from treating a stopped demo as a failure.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      MODE="start"
      shift
      ;;
    --stop)
      MODE="stop"
      shift
      ;;
    --status)
      MODE="status"
      shift
      ;;
    --duration-minutes)
      DURATION_MINUTES="${2:-}"
      shift 2
      ;;
    --max-events)
      MAX_EVENTS="${2:-}"
      shift 2
      ;;
    --rate)
      RATE="${2:-}"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="${2:-}"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:-}"
      shift 2
      ;;
    --submit-timeout)
      SUBMIT_TIMEOUT="${2:-}"
      shift 2
      ;;
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --keep-running)
      AUTO_STOP=0
      shift
      ;;
    --skip-risk)
      WITH_RISK=0
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

case "$DURATION_MINUTES:$MAX_EVENTS:$RATE:$BATCH_SIZE:$WAIT_SECONDS:$SUBMIT_TIMEOUT" in
  *[!0-9:]*|"")
    echo "numeric arguments must be non-negative integers" >&2
    exit 2
    ;;
esac

p2_init "p6_realtime_demo" "${ORIGINAL_ARGS[@]}"
p2_header "P6 MetroPT realtime demo mode"
printf 'mode=%s\nduration_minutes=%s\nmax_events=%s\nrate=%s\nwait_seconds=%s\nauto_stop=%s\nconfig=%s\n\n' \
  "$MODE" "$DURATION_MINUTES" "$MAX_EVENTS" "$RATE" "$WAIT_SECONDS" "$AUTO_STOP" "$CONFIG"
printf 'with_risk=%s\n\n' "$WITH_RISK"

DEMO_STATUS="$P2_RUN_DIR/demo_status.tsv"
DEMO_SUMMARY_MD="$P2_RUN_DIR/demo_summary.md"
DEMO_SUMMARY_JSON="$P2_RUN_DIR/demo_summary.json"
FLINK_JOBS_BEFORE="$P2_RUN_DIR/flink_jobs_before.log"
FLINK_JOBS_DURING="$P2_RUN_DIR/flink_jobs_during.log"
FLINK_JOBS_AFTER="$P2_RUN_DIR/flink_jobs_after.log"
FLINK_JOBS_STATUS="$P2_RUN_DIR/flink_jobs_status.log"
REDIS_SAMPLE="$P2_RUN_DIR/redis_kpi_sample.log"
HIVE_SAMPLE="$P2_RUN_DIR/hive_realtime_sample.log"
REDIS_RISK_SAMPLE="$P2_RUN_DIR/redis_risk_sample.log"
HIVE_RISK_SAMPLE="$P2_RUN_DIR/hive_realtime_risk_sample.log"

printf 'key\tvalue\n' > "$DEMO_STATUS"

status_kv() {
  local key="$1"
  local value="$2"
  printf '%s\t%s\n' "$key" "$value" >> "$DEMO_STATUS"
}

flink_list() {
  local out="$1"
  bash -lc "export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink; /export/server/flink/bin/flink list 2>&1 || true" > "$out"
}

extract_job_ids() {
  local file="$1"
  grep -Eo '[0-9a-f]{32}' "$file" 2>/dev/null | sort -u | tr '\n' ' ' | sed 's/[[:space:]]*$//'
}

latest_completed_demo() {
  ls -1dt "$P2_VALIDATION_ROOT"/p6_realtime_demo_* 2>/dev/null | grep -v "$P2_RUN_DIR" | head -n 1 || true
}

read_status_value() {
  local file="$1"
  local key="$2"
  awk -F'\t' -v key="$key" '$1==key {value=$2} END{print value}' "$file" 2>/dev/null || true
}

cancel_job_id() {
  local job_id="$1"
  local log="$P2_RUN_DIR/cancel_${job_id}.log"
  p2_run_logged "cancel_flink_${job_id}" "Flink" "$log" \
    "Flink cancel failed for managed demo job." \
    "export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list; /export/server/flink/bin/flink cancel $job_id" \
    bash -lc "export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink; /export/server/flink/bin/flink cancel '$job_id'"
}

capture_redis_sample() {
  if redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' 2>/dev/null | head -n 20 > "$REDIS_SAMPLE"; then
    local count first
    count="$(wc -l < "$REDIS_SAMPLE" | tr -d ' ')"
    first="$(head -n 1 "$REDIS_SAMPLE")"
    if [[ "$count" -gt 0 ]]; then
      {
        printf '\n[first_key]\n%s\n' "$first"
        redis-cli -h hadoop1 HGETALL "$first" 2>/dev/null || true
      } >> "$REDIS_SAMPLE"
      p2_report PASS "redis_kpi_sample" "keys_sampled=$count log=$REDIS_SAMPLE"
      status_kv "redis_kpi_key_sample_count" "$count"
    else
      p2_report WARN "redis_kpi_sample" "no metropt:kpi:1m:* keys observed"
      status_kv "redis_kpi_key_sample_count" "0"
    fi
  else
    p2_report WARN "redis_kpi_sample" "redis scan failed log=$REDIS_SAMPLE"
    status_kv "redis_kpi_key_sample_count" "0"
  fi
}

capture_redis_risk_sample() {
  if redis-cli -h hadoop1 --scan --pattern 'metropt_quality:risk:latest:*' 2>/dev/null | head -n 20 > "$REDIS_RISK_SAMPLE"; then
    local count first
    count="$(wc -l < "$REDIS_RISK_SAMPLE" | tr -d ' ')"
    first="$(head -n 1 "$REDIS_RISK_SAMPLE")"
    if [[ "$count" -gt 0 ]]; then
      {
        printf '\n[first_key]\n%s\n' "$first"
        redis-cli -h hadoop1 HGETALL "$first" 2>/dev/null || true
      } >> "$REDIS_RISK_SAMPLE"
      if grep -q 'risk_score' "$REDIS_RISK_SAMPLE" && grep -q 'risk_level' "$REDIS_RISK_SAMPLE"; then
        p2_report PASS "redis_risk_sample" "keys_sampled=$count log=$REDIS_RISK_SAMPLE"
        status_kv "redis_risk_key_sample_count" "$count"
      else
        p2_report WARN "redis_risk_sample" "risk fields missing in sampled key log=$REDIS_RISK_SAMPLE"
        status_kv "redis_risk_key_sample_count" "$count"
      fi
    else
      p2_report WARN "redis_risk_sample" "no metropt_quality:risk:latest:* keys observed"
      status_kv "redis_risk_key_sample_count" "0"
    fi
  else
    p2_report WARN "redis_risk_sample" "redis scan failed log=$REDIS_RISK_SAMPLE"
    status_kv "redis_risk_key_sample_count" "0"
  fi
}

capture_hive_sample() {
  local sql="$P2_RUN_DIR/hive_realtime_sample.sql"
  cat > "$sql" <<'SQL'
USE metropt_quality;
SHOW TABLES LIKE '*realtime*';
SHOW PARTITIONS ods_metropt_realtime_readings;
SELECT * FROM ods_metropt_realtime_readings LIMIT 5;
SELECT * FROM dws_metropt_realtime_kpi_1min LIMIT 5;
SQL
  if bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$sql'" > "$HIVE_SAMPLE" 2>&1; then
    if grep -q 'ods_metropt_realtime_readings' "$HIVE_SAMPLE"; then
      p2_report PASS "hive_realtime_sample" "log=$HIVE_SAMPLE"
      status_kv "hive_realtime_sample" "PASS"
    else
      p2_report WARN "hive_realtime_sample" "expected realtime table text missing log=$HIVE_SAMPLE"
      status_kv "hive_realtime_sample" "WARN"
    fi
  else
    p2_report WARN "hive_realtime_sample" "beeline failed log=$HIVE_SAMPLE"
    status_kv "hive_realtime_sample" "WARN"
  fi
}

capture_hive_risk_sample() {
  local sql="$P2_RUN_DIR/hive_realtime_risk_sample.sql"
  cat > "$sql" <<'SQL'
USE metropt_quality;
SHOW TABLES LIKE '*risk*';
SHOW PARTITIONS dws_metropt_realtime_risk_events;
SELECT event_id,event_time,operating_state,risk_score,risk_level,risk_reason,model_version
FROM dws_metropt_realtime_risk_events
LIMIT 5;
SQL
  if bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$sql'" > "$HIVE_RISK_SAMPLE" 2>&1; then
    if grep -q 'dws_metropt_realtime_risk_events' "$HIVE_RISK_SAMPLE" && grep -q 'risk_score' "$HIVE_RISK_SAMPLE"; then
      p2_report PASS "hive_realtime_risk_sample" "log=$HIVE_RISK_SAMPLE"
      status_kv "hive_realtime_risk_sample" "PASS"
    else
      p2_report WARN "hive_realtime_risk_sample" "expected risk table/field text missing log=$HIVE_RISK_SAMPLE"
      status_kv "hive_realtime_risk_sample" "WARN"
    fi
  else
    p2_report WARN "hive_realtime_risk_sample" "beeline failed log=$HIVE_RISK_SAMPLE"
    status_kv "hive_realtime_risk_sample" "WARN"
  fi
}

write_summary_files() {
  local overall="${1:-PASS}"
  local current_state="${2:-unknown}"
  local latest_p1="${3:-}"
  local managed_jobs="${4:-}"
  local sent="${5:-}"
  local failed="${6:-}"
  local latest_p11="${7:-}"

  status_kv "overall_status" "$overall"
  status_kv "current_state" "$current_state"
  status_kv "p1_log_dir" "$latest_p1"
  status_kv "p11_risk_log_dir" "${latest_p11:-N/A}"
  status_kv "managed_job_ids" "${managed_jobs:-N/A}"
  status_kv "replay_sent" "${sent:-N/A}"
  status_kv "replay_failed" "${failed:-N/A}"
  status_kv "ended_at" "$(date '+%F %T')"

  cat > "$DEMO_SUMMARY_MD" <<EOF
# MetroPT P6 Realtime Demo Summary

- run_id: $P2_RUN_ID
- overall_status: $overall
- current_state: $current_state
- duration_minutes: $DURATION_MINUTES
- max_events: $MAX_EVENTS
- rate: $RATE
- p1_log_dir: ${latest_p1:-N/A}
- p11_risk_log_dir: ${latest_p11:-N/A}
- managed_job_ids: ${managed_jobs:-N/A}
- replay_sent: ${sent:-N/A}
- replay_failed: ${failed:-N/A}

## Evidence

- demo_status: \`$DEMO_STATUS\`
- flink_jobs_before: \`$FLINK_JOBS_BEFORE\`
- flink_jobs_during: \`$FLINK_JOBS_DURING\`
- flink_jobs_after: \`$FLINK_JOBS_AFTER\`
- redis_sample: \`$REDIS_SAMPLE\`
- hive_sample: \`$HIVE_SAMPLE\`
- redis_risk_sample: \`$REDIS_RISK_SAMPLE\`
- hive_risk_sample: \`$HIVE_RISK_SAMPLE\`
EOF

  python3 - "$DEMO_SUMMARY_JSON" "$DEMO_STATUS" <<'PY'
import json
import sys
from pathlib import Path

out, status_path = sys.argv[1:]
payload = {}
for line in Path(status_path).read_text(encoding="utf-8").splitlines()[1:]:
    if "\t" in line:
        k, v = line.split("\t", 1)
        payload[k] = v
Path(out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
  p2_report PASS "demo_summary" "summary=$DEMO_SUMMARY_MD json=$DEMO_SUMMARY_JSON"
}

mode_status() {
  local latest status_file latest_status latest_state latest_jobs current_jobs
  latest="$(latest_completed_demo)"
  flink_list "$FLINK_JOBS_STATUS"
  current_jobs="$(extract_job_ids "$FLINK_JOBS_STATUS")"
  status_kv "mode" "status"
  status_kv "latest_demo_run" "${latest:-N/A}"
  status_kv "current_flink_job_ids" "${current_jobs:-N/A}"
  if [[ -n "$latest" && -f "$latest/demo_status.tsv" ]]; then
    status_file="$latest/demo_status.tsv"
    latest_status="$(read_status_value "$status_file" overall_status)"
    latest_state="$(read_status_value "$status_file" current_state)"
    latest_jobs="$(read_status_value "$status_file" managed_job_ids)"
    status_kv "latest_demo_overall_status" "${latest_status:-N/A}"
    status_kv "latest_demo_state" "${latest_state:-N/A}"
    status_kv "latest_demo_managed_job_ids" "${latest_jobs:-N/A}"
    p2_report PASS "latest_demo" "run=$latest status=${latest_status:-N/A} state=${latest_state:-N/A}"
  else
    p2_report SKIP "latest_demo" "no previous P6 demo run found"
  fi
  if [[ -n "$current_jobs" ]]; then
    p2_report PASS "flink_current_jobs" "job_ids=$current_jobs log=$FLINK_JOBS_STATUS"
  else
    p2_report SKIP "flink_current_jobs" "no running Flink jobs log=$FLINK_JOBS_STATUS"
  fi
  capture_redis_sample
  capture_hive_sample
  capture_redis_risk_sample
  capture_hive_risk_sample
  write_summary_files "PASS" "status_snapshot" "${latest:-}" "$current_jobs" "" "" ""
}

mode_stop() {
  local latest status_file managed_jobs current_state
  latest="$(latest_completed_demo)"
  status_kv "mode" "stop"
  status_kv "target_demo_run" "${latest:-N/A}"
  if [[ -z "$latest" || ! -f "$latest/demo_status.tsv" ]]; then
    p2_report SKIP "stop_target" "no previous P6 demo run found"
    flink_list "$FLINK_JOBS_AFTER"
    write_summary_files "PASS" "not_running" "" "" "" ""
    return 0
  fi
  status_file="$latest/demo_status.tsv"
  managed_jobs="$(read_status_value "$status_file" managed_job_ids)"
  if [[ -z "$managed_jobs" || "$managed_jobs" == "N/A" ]]; then
    p2_report SKIP "stop_target" "latest demo has no managed job ids"
  else
    local job
    for job in $managed_jobs; do
      cancel_job_id "$job" || true
    done
  fi
  flink_list "$FLINK_JOBS_AFTER"
  if [[ -n "$(extract_job_ids "$FLINK_JOBS_AFTER")" ]]; then
    current_state="running_jobs_present"
  else
    current_state="not_running"
  fi
  capture_redis_sample
  capture_hive_sample
  capture_redis_risk_sample
  capture_hive_risk_sample
  write_summary_files "PASS" "$current_state" "$latest" "$managed_jobs" "" "" ""
}

mode_start() {
  local group_id="p6_demo_${P2_RUN_ID}"
  local p1_log="$P2_RUN_DIR/p1_realtime_acceptance.log"
  local p11_log="$P2_RUN_DIR/p11_realtime_risk_acceptance.log"
  local p1_log_dir p11_log_dir sent failed before_jobs during_jobs after_jobs managed_jobs current_state overall flink_submit_log flink_state
  status_kv "mode" "start"
  status_kv "started_at" "$(date '+%F %T')"
  status_kv "group_id" "$group_id"
  status_kv "with_risk" "$WITH_RISK"

  p2_run_logged "ensure_realtime_mode" "Realtime" "$P2_RUN_DIR/ensure_realtime_mode.log" \
    "Realtime base services are not ready." \
    "cd $P2_PROJECT_ROOT && bin/start_realtime_mode.sh" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_realtime_mode.sh"

  flink_list "$FLINK_JOBS_BEFORE"
  before_jobs="$(extract_job_ids "$FLINK_JOBS_BEFORE")"
  status_kv "flink_job_ids_before" "${before_jobs:-N/A}"

  p2_run_logged "p1_realtime_acceptance" "Realtime" "$p1_log" \
    "P1 realtime acceptance failed during P6 demo." \
    "cd $P2_PROJECT_ROOT && bin/p1_metropt_realtime_acceptance.sh --max-events $MAX_EVENTS --rate $RATE" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p1_metropt_realtime_acceptance.sh --config '$CONFIG' --max-events '$MAX_EVENTS' --rate '$RATE' --batch-size '$BATCH_SIZE' --wait-seconds '$WAIT_SECONDS' --submit-timeout '$SUBMIT_TIMEOUT' --startup-mode earliest-offset --group-id '$group_id'"

  p1_log_dir="$(grep -m1 '^log_dir=' "$p1_log" 2>/dev/null | cut -d= -f2- || true)"
  flink_submit_log="$p1_log_dir/flink_submit.log"
  sent="$(grep 'replay_send_summary' "$p1_log" 2>/dev/null | tail -n 1 | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p')"
  failed="$(grep 'replay_send_summary' "$p1_log" 2>/dev/null | tail -n 1 | sed -n 's/.*failed=\([0-9][0-9]*\).*/\1/p')"
  status_kv "p1_acceptance_log" "$p1_log"
  status_kv "flink_submit_log" "${flink_submit_log:-N/A}"

  if ((WITH_RISK == 1)); then
    p2_run_logged "p11_realtime_risk_acceptance" "RealtimeRisk" "$p11_log" \
      "P11 realtime risk acceptance failed during P6 demo." \
      "cd $P2_PROJECT_ROOT && bin/p11_realtime_risk_acceptance.sh --max-events $MAX_EVENTS --rate $RATE" \
      bash -lc "cd '$P2_PROJECT_ROOT' && bin/p11_realtime_risk_acceptance.sh --config '$CONFIG' --max-events '$MAX_EVENTS' --rate '$RATE' --batch-size '$BATCH_SIZE' --wait-seconds '$WAIT_SECONDS' --submit-timeout '$SUBMIT_TIMEOUT' --startup-mode earliest-offset --group-id '${group_id}_risk'"
    p11_log_dir="$(grep -m1 '^run_dir=' "$p11_log" 2>/dev/null | cut -d= -f2- || true)"
    status_kv "p11_risk_acceptance_log" "$p11_log"
    status_kv "p11_risk_log_dir" "${p11_log_dir:-N/A}"
  else
    p2_report WARN "p11_realtime_risk_acceptance" "skipped by --skip-risk; P6 will only capture existing risk samples if present"
    status_kv "p11_risk_acceptance_log" "SKIPPED"
    p11_log_dir=""
  fi

  flink_list "$FLINK_JOBS_DURING"
  during_jobs="$(extract_job_ids "$FLINK_JOBS_DURING")"
  managed_jobs="$(comm -13 <(printf '%s\n' $before_jobs | sort -u) <(printf '%s\n' $during_jobs | sort -u) | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -z "$managed_jobs" && -n "$during_jobs" && "$during_jobs" != "$before_jobs" ]]; then
    managed_jobs="$during_jobs"
  fi
  status_kv "flink_job_ids_during" "${during_jobs:-N/A}"
  status_kv "managed_job_ids_detected" "${managed_jobs:-N/A}"

  if [[ -n "$during_jobs" ]]; then
    flink_state="running"
    p2_report PASS "flink_demo_job_state" "running job_ids=$during_jobs log=$FLINK_JOBS_DURING"
  elif [[ -n "$flink_submit_log" && -f "$flink_submit_log" ]] && grep -q 'MetroPT Flink 作业已提交' "$flink_submit_log"; then
    flink_state="submitted_then_not_running"
    p2_report PASS "flink_demo_job_state" "submit marker observed; no running job after bounded demo processing log=$FLINK_JOBS_DURING"
  else
    flink_state="not_observed"
    p2_report WARN "flink_demo_job_state" "no running job observed and submit marker missing log=$FLINK_JOBS_DURING"
  fi
  status_kv "flink_job_observed_state" "$flink_state"

  if ((DURATION_MINUTES > 0)); then
    local total_seconds=$((DURATION_MINUTES * 60))
    local end_time=$((SECONDS + total_seconds))
    local sample_id=0
    while ((SECONDS < end_time)); do
      sample_id=$((sample_id + 1))
      flink_list "$P2_RUN_DIR/flink_jobs_sample_${sample_id}.log"
      sleep 30
    done
  fi

  capture_redis_sample
  capture_hive_sample
  capture_redis_risk_sample
  capture_hive_risk_sample

  if ((AUTO_STOP == 1)); then
    if [[ -n "$managed_jobs" ]]; then
      local job
      for job in $managed_jobs; do
        cancel_job_id "$job" || true
      done
    else
      p2_report PASS "cancel_flink_demo_jobs" "no managed running job ids detected; nothing to cancel"
    fi
  else
    p2_report WARN "cancel_flink_demo_jobs" "skipped by --keep-running"
  fi

  flink_list "$FLINK_JOBS_AFTER"
  after_jobs="$(extract_job_ids "$FLINK_JOBS_AFTER")"
  if [[ -n "$after_jobs" ]]; then
    current_state="running"
  else
    current_state="not_running"
  fi
  overall="PASS"
  if [[ "${failed:-0}" != "0" ]]; then
    overall="WARN"
  fi
  write_summary_files "$overall" "$current_state" "$p1_log_dir" "$managed_jobs" "$sent" "$failed" "$p11_log_dir"
}

case "$MODE" in
  start) mode_start ;;
  stop) mode_stop ;;
  status) mode_status ;;
  *)
    echo "invalid mode: $MODE" >&2
    exit 2
    ;;
esac

p2_finish

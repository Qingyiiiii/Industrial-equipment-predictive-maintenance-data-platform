#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

CONFIG="/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml"
PYTHON_EXEC="${PYTHON_EXEC:-python3}"
FLINK_PYTHON="${FLINK_PYTHON:-/export/server/venv/flink120/bin/python}"
MAX_EVENTS=10000
RATE=500
BATCH_SIZE=500
STARTUP_MODE="earliest-offset"
GROUP_ID=""
SUBMIT_TIMEOUT=120
WAIT_SECONDS=90
INJECT_DLQ_TEST=0
SKIP_FLINK_SUBMIT=0
ORIGINAL_ARGS=("$@")

KAFKA_TOPIC="metropt.ods.compressor.reading.v1"
DLQ_TOPIC="metropt.ods.compressor.reading.dlq.v1"
RISK_TABLE="dws_metropt_realtime_risk_events"
RISK_REDIS_PATTERN="metropt_quality:risk:latest:*"

usage() {
  cat <<'USAGE'
Usage:
  bin/p11_realtime_risk_acceptance.sh [options]

Options:
  --config PATH              MetroPT cluster config.
  --max-events N             Replay event count, default: 10000.
  --rate N                   Replay events per second, default: 500.
  --batch-size N             Replay batch size, default: 500.
  --startup-mode MODE        Flink startup mode, default: earliest-offset.
  --group-id ID              Flink Kafka group id, default: p11_risk_<run_id>.
  --submit-timeout SECONDS   Timeout for streaming job submit command, default: 120.
  --wait-seconds SECONDS     Wait after submit before Hive/Redis checks, default: 90.
  --inject-dlq-test          Send one malformed event and verify the DLQ path.
  --skip-flink-submit        Do not submit a new risk job; verify current running job/output only.
  -h, --help                 Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
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
    --startup-mode)
      STARTUP_MODE="${2:-}"
      shift 2
      ;;
    --group-id)
      GROUP_ID="${2:-}"
      shift 2
      ;;
    --submit-timeout)
      SUBMIT_TIMEOUT="${2:-}"
      shift 2
      ;;
    --wait-seconds)
      WAIT_SECONDS="${2:-}"
      shift 2
      ;;
    --inject-dlq-test)
      INJECT_DLQ_TEST=1
      shift
      ;;
    --skip-flink-submit)
      SKIP_FLINK_SUBMIT=1
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

case "$MAX_EVENTS:$RATE:$BATCH_SIZE:$SUBMIT_TIMEOUT:$WAIT_SECONDS" in
  *[!0-9:]*|"")
    echo "numeric arguments must be non-negative integers" >&2
    exit 2
    ;;
esac

p2_init "p11_realtime_risk" "${ORIGINAL_ARGS[@]}"
GROUP_ID="${GROUP_ID:-p11_risk_${P2_RUN_ID}}"
p2_header "P11 MetroPT realtime risk acceptance"
printf 'group_id=%s\nmax_events=%s\nrate=%s\nwait_seconds=%s\nrisk_table=%s\nredis_pattern=%s\nconfig=%s\n\n' \
  "$GROUP_ID" "$MAX_EVENTS" "$RATE" "$WAIT_SECONDS" "$RISK_TABLE" "$RISK_REDIS_PATTERN" "$CONFIG"

RISK_STATUS="$P2_RUN_DIR/risk_status.tsv"
RISK_SUMMARY_MD="$P2_RUN_DIR/risk_summary.md"
RISK_SUMMARY_JSON="$P2_RUN_DIR/risk_summary.json"
DLQ_MARKER="p11_dlq_${P2_RUN_ID}"
printf 'key\tvalue\n' > "$RISK_STATUS"

status_kv() {
  local key="$1"
  local value="$2"
  printf '%s\t%s\n' "$key" "$value" >> "$RISK_STATUS"
}

run_required_gate() {
  local step="$1"
  local cmd="$2"
  if ! p2_run_logged "$step" "P0" "$P2_RUN_DIR/${step}.log" \
    "P0 prerequisite failed; fix base realtime services before P11 risk acceptance." \
    "cd $P2_PROJECT_ROOT && $cmd" \
    bash -lc "cd '$P2_PROJECT_ROOT' && $cmd"; then
    p2_finish
  fi
}

run_required_gate "p0_kafka" "bin/p0_cluster_health_check.sh --module kafka"
run_required_gate "p0_redis_flink" "bin/p0_cluster_health_check.sh --module redis-flink"
run_required_gate "p0_hive" "bin/p0_cluster_health_check.sh --module hive"

wait_for_redis_pattern() {
  local pattern="$1"
  local label="$2"
  local max_seconds="$3"
  local interval=5
  local elapsed=0
  local key=""
  if ((max_seconds <= 0)); then
    return 0
  fi
  while ((elapsed < max_seconds)); do
    key="$(redis-cli -h 192.168.88.101 -p 6379 --scan --pattern "$pattern" 2>/dev/null | head -n 1 || true)"
    if [[ -n "$key" ]]; then
      printf '[INFO] %s visible after %ss redis_key=%s\n' "$label" "$elapsed" "$key"
      return 0
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done
  printf '[INFO] %s not visible after %ss; continuing to Hive/Redis checks\n' "$label" "$max_seconds"
}

p2_run_logged "replay_dry_run" "Kafka" "$P2_RUN_DIR/replay_dry_run.log" \
  "MetroPT replay dry-run failed; source CSV or config is invalid." \
  "cd $P2_PROJECT_ROOT && $PYTHON_EXEC streaming/metropt_replay_to_kafka.py --config '$CONFIG' --dry-run --print-sample 3 --max-events 3" \
  bash -lc "cd '$P2_PROJECT_ROOT' && '$PYTHON_EXEC' streaming/metropt_replay_to_kafka.py --config '$CONFIG' --dry-run --print-sample 3 --max-events 3"

send_log="$P2_RUN_DIR/replay_send.log"
p2_run_logged "kafka_input_replay_send" "Kafka" "$send_log" \
  "MetroPT replay send failed; check Kafka broker health and kafka-python dependency." \
  "bin/p0_cluster_health_check.sh --module kafka; tail -n 120 '$send_log'" \
  bash -lc "cd '$P2_PROJECT_ROOT' && '$PYTHON_EXEC' streaming/metropt_replay_to_kafka.py --config '$CONFIG' --rate '$RATE' --batch-size '$BATCH_SIZE' --max-events '$MAX_EVENTS'"

sent=""
failed=""
if grep -q 'MetroPT replay 完成:' "$send_log"; then
  sent="$(grep 'MetroPT replay 完成:' "$send_log" | tail -n 1 | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p')"
  failed="$(grep 'MetroPT replay 完成:' "$send_log" | tail -n 1 | sed -n 's/.*failed=\([0-9][0-9]*\).*/\1/p')"
  status_kv "replay_sent" "${sent:-0}"
  status_kv "replay_failed" "${failed:-1}"
  if [[ "${sent:-0}" -gt 0 && "${failed:-1}" -eq 0 ]]; then
    p2_report PASS "kafka_input_replay_summary" "sent=$sent failed=$failed"
  else
    p2_report FAIL "kafka_input_replay_summary" "unexpected sent/failed values in $send_log"
  fi
fi

if ((INJECT_DLQ_TEST == 1)); then
  dlq_preinject_log="$P2_RUN_DIR/dlq_preinject_bad_event.log"
  status_kv "dlq_marker" "$DLQ_MARKER"
  p2_run_logged "dlq_preinject_bad_event" "Kafka" "$dlq_preinject_log" \
    "Malformed event injection failed before Flink risk submit." \
    "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-producer.sh --bootstrap-server 192.168.88.101:9092 --topic '$KAFKA_TOPIC'" \
    bash -lc "export JAVA_HOME=/export/server/jdk17; printf '%s\n' '{\"event_id\":null,\"raw_index\":null,\"source\":\"$DLQ_MARKER\"}' | /export/server/kafka/bin/kafka-console-producer.sh --bootstrap-server 192.168.88.101:9092 --topic '$KAFKA_TOPIC'"
fi

if ((SKIP_FLINK_SUBMIT == 1)); then
  p2_report WARN "flink_risk_submit" "skipped by --skip-flink-submit"
else
  flink_log="$P2_RUN_DIR/flink_risk_submit.log"
  mkdir -p "$(dirname "$flink_log")"
  printf '[START] flink_risk_submit -> %s\n' "$flink_log"
  bash -lc "cd '$P2_PROJECT_ROOT' && export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink HIVE_CONF_DIR=/export/server/hive/conf METROPT_CONFIG='$CONFIG' && export PATH=/usr/local/bin:/usr/bin:/bin:\$JAVA_HOME/bin:\$FLINK_HOME/bin:\$PATH && timeout '$SUBMIT_TIMEOUT' '$FLINK_PYTHON' streaming/02_flink_metropt_realtime_risk_score.py --startup-mode '$STARTUP_MODE' --group-id '$GROUP_ID'" > "$flink_log" 2>&1
  flink_rc=$?
  if ((flink_rc == 0)) || grep -q 'MetroPT P11 Flink 风险作业已提交' "$flink_log"; then
    p2_report PASS "flink_risk_submit" "submit observed rc=$flink_rc log=$flink_log"
    status_kv "flink_submit_log" "$flink_log"
  else
    p2_report FAIL "flink_risk_submit" "rc=$flink_rc log=$flink_log"
    p2_failure_template "flink_risk_submit" "Flink" "$flink_rc" "$flink_log" "" \
      "Flink risk scoring job submit failed." \
      "/export/server/flink/bin/flink list; tail -n 120 /export/server/flink/log/*"
  fi
fi

p2_run_logged "flink_running_jobs" "Flink" "$P2_RUN_DIR/flink_running_jobs.log" \
  "Flink CLI cannot list jobs or no JobManager is reachable." \
  "bin/p0_cluster_health_check.sh --module redis-flink; /export/server/flink/bin/flink list" \
  bash -lc "export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink; /export/server/flink/bin/flink list"

if ((WAIT_SECONDS > 0)); then
  printf '[INFO] wait up to %s seconds for Flink/Hive/Redis risk visibility\n' "$WAIT_SECONDS"
  wait_for_redis_pattern "$RISK_REDIS_PATTERN" "realtime risk" "$WAIT_SECONDS"
fi

hive_sql="$P2_RUN_DIR/realtime_risk_hive_check.sql"
cat > "$hive_sql" <<SQL
USE metropt_quality;
SHOW TABLES LIKE '*risk*';
DESCRIBE FORMATTED $RISK_TABLE;
SHOW PARTITIONS $RISK_TABLE;
SELECT event_id,event_time,operating_state,risk_score,risk_level,risk_reason,model_version FROM $RISK_TABLE LIMIT 10;
SELECT risk_level, COUNT(*) FROM $RISK_TABLE GROUP BY risk_level;
SQL

hive_log="$P2_RUN_DIR/realtime_risk_hive_check.log"
p2_run_logged "hive_risk_query" "Hive" "$hive_log" \
  "Realtime risk Hive Beeline check failed." \
  "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common -f '$hive_sql'" \
  bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$hive_sql'"

if grep -q "$RISK_TABLE" "$hive_log" && grep -q 'risk_score' "$hive_log"; then
  p2_report PASS "hive_risk_fields_visible" "risk table and fields queried log=$hive_log"
  status_kv "hive_risk_query" "PASS"
else
  p2_report FAIL "hive_risk_fields_visible" "risk fields missing in $hive_log"
  status_kv "hive_risk_query" "FAIL"
fi

redis_log="$P2_RUN_DIR/redis_risk_check.log"
p2_run_logged "redis_risk_check" "Redis" "$redis_log" \
  "Redis risk latest key is missing." \
  "redis-cli -h 192.168.88.101 -p 6379 --scan --pattern '$RISK_REDIS_PATTERN' | head" \
  bash -lc "KEYS=\$(redis-cli -h 192.168.88.101 -p 6379 --scan --pattern '$RISK_REDIS_PATTERN' | head -n 10); test -n \"\$KEYS\"; echo \"\$KEYS\"; FIRST=\$(printf '%s\n' \"\$KEYS\" | head -n 1); test -n \"\$FIRST\" && redis-cli -h 192.168.88.101 -p 6379 HGETALL \"\$FIRST\""

if grep -q 'risk_score' "$redis_log" && grep -q 'risk_level' "$redis_log"; then
  p2_report PASS "redis_risk_fields_visible" "risk_score/risk_level present log=$redis_log"
  status_kv "redis_risk_query" "PASS"
else
  p2_report FAIL "redis_risk_fields_visible" "risk fields missing in $redis_log"
  status_kv "redis_risk_query" "FAIL"
fi

dlq_normal_log="$P2_RUN_DIR/dlq_normal_check.log"
timeout 15 bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server 192.168.88.101:9092 --topic '$DLQ_TOPIC' --max-messages 1 --timeout-ms 10000" > "$dlq_normal_log" 2>&1
dlq_normal_rc=$?
if [[ -s "$dlq_normal_log" ]] && grep -q '{' "$dlq_normal_log"; then
  p2_report WARN "dlq_normal_check" "DLQ message observed while checking latest offset rc=$dlq_normal_rc log=$dlq_normal_log"
else
  p2_report PASS "dlq_normal_check" "no DLQ message observed at latest offset for normal replay log=$dlq_normal_log"
fi

if ((INJECT_DLQ_TEST == 1)); then
  inject_log="$P2_RUN_DIR/dlq_inject_check.log"
  timeout 45 bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server 192.168.88.101:9092 --topic '$DLQ_TOPIC' --from-beginning --max-messages 100 --timeout-ms 30000" > "$inject_log" 2>&1
  if grep -q "$DLQ_MARKER" "$inject_log"; then
    p2_report PASS "dlq_inject_check" "DLQ message observed for marker=$DLQ_MARKER log=$inject_log"
  else
    p2_report FAIL "dlq_inject_check" "no DLQ message observed for marker=$DLQ_MARKER log=$inject_log"
  fi
else
  p2_report WARN "dlq_inject_check" "skipped; pass --inject-dlq-test to force bad event validation"
fi

status_kv "ended_at" "$(date '+%F %T')"
cat > "$RISK_SUMMARY_MD" <<EOF
# MetroPT P11 Realtime Risk Acceptance Summary

- run_id: $P2_RUN_ID
- group_id: $GROUP_ID
- replay_sent: ${sent:-N/A}
- replay_failed: ${failed:-N/A}
- risk_table: metropt_quality.$RISK_TABLE
- redis_pattern: $RISK_REDIS_PATTERN

## Evidence

- risk_status: \`$RISK_STATUS\`
- replay_send: \`$send_log\`
- flink_risk_submit: \`${flink_log:-N/A}\`
- hive_risk_check: \`$hive_log\`
- redis_risk_check: \`$redis_log\`
- dlq_normal_check: \`$dlq_normal_log\`
EOF

python3 - "$RISK_SUMMARY_JSON" "$RISK_STATUS" <<'PY'
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
p2_report PASS "risk_summary" "summary=$RISK_SUMMARY_MD json=$RISK_SUMMARY_JSON"

p2_finish

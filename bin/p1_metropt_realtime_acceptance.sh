#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p1_common.sh"

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

KAFKA_TOPIC="metropt.ods.compressor.reading.v1"
DLQ_TOPIC="metropt.ods.compressor.reading.dlq.v1"
BOOTSTRAP="192.168.88.101:9092,192.168.88.102:9092,192.168.88.103:9092"
REDIS_PATTERN="metropt:kpi:1m:*"

usage() {
  cat <<'USAGE'
Usage:
  bin/p1_metropt_realtime_acceptance.sh [options]

Options:
  --config PATH              MetroPT cluster config.
  --max-events N             Replay event count, default: 10000.
  --rate N                   Replay events per second, default: 500.
  --batch-size N             Replay batch size, default: 500.
  --startup-mode MODE        Flink startup mode, default: earliest-offset.
  --group-id ID              Flink Kafka group id, default: p1_metropt_<run_id>.
  --submit-timeout SECONDS   Timeout for streaming job submit command, default: 120.
  --wait-seconds SECONDS     Wait after replay before Hive/Redis checks, default: 90.
  --inject-dlq-test          Send one malformed event and verify DLQ has at least one message.
  --skip-flink-submit        Do not submit a new Flink job; verify current running job only.
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
  *[!0-9:]*)
    echo "numeric arguments must be non-negative integers" >&2
    exit 2
    ;;
esac

p1_init "realtime_acceptance"
GROUP_ID="${GROUP_ID:-p1_metropt_${P1_RUN_ID}}"
p1_header "P1 MetroPT realtime acceptance"
printf 'group_id=%s\nmax_events=%s\nrate=%s\nwait_seconds=%s\n\n' "$GROUP_ID" "$MAX_EVENTS" "$RATE" "$WAIT_SECONDS"

run_required_gate() {
  local step="$1"
  local cmd="$2"
  if ! p1_run_p0_gate "$step" "$cmd"; then
    p1_failure_template "$step" "P0" 3 "$P1_LOG_DIR/${step}.log" "" \
      "P0 prerequisite failed." \
      "cd $P1_PROJECT_ROOT && $cmd"
    exit 3
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

p1_run_logged "replay_dry_run" "Kafka" "$P1_LOG_DIR/replay_dry_run.log" \
  "MetroPT replay dry-run failed; source CSV or config is invalid." \
  "cd $P1_PROJECT_ROOT && $PYTHON_EXEC streaming/metropt_replay_to_kafka.py --config '$CONFIG' --dry-run --print-sample 3 --max-events 3" \
  bash -lc "cd '$P1_PROJECT_ROOT' && '$PYTHON_EXEC' streaming/metropt_replay_to_kafka.py --config '$CONFIG' --dry-run --print-sample 3 --max-events 3"

send_log="$P1_LOG_DIR/replay_send.log"
p1_run_logged "replay_send" "Kafka" "$send_log" \
  "MetroPT replay send failed; check Kafka broker health and kafka-python dependency." \
  "bin/p0_cluster_health_check.sh --module kafka; tail -n 120 '$send_log'" \
  bash -lc "cd '$P1_PROJECT_ROOT' && '$PYTHON_EXEC' streaming/metropt_replay_to_kafka.py --config '$CONFIG' --rate '$RATE' --batch-size '$BATCH_SIZE' --max-events '$MAX_EVENTS'"

if grep -q 'MetroPT replay 完成:' "$send_log"; then
  sent="$(grep 'MetroPT replay 完成:' "$send_log" | tail -n 1 | sed -n 's/.*sent=\([0-9][0-9]*\).*/\1/p')"
  failed="$(grep 'MetroPT replay 完成:' "$send_log" | tail -n 1 | sed -n 's/.*failed=\([0-9][0-9]*\).*/\1/p')"
  if [[ "${sent:-0}" -gt 0 && "${failed:-1}" -eq 0 ]]; then
    p1_report PASS "replay_send_summary" "sent=$sent failed=$failed"
  else
    p1_report FAIL "replay_send_summary" "unexpected sent/failed values in $send_log"
    p1_failure_template "replay_send_summary" "Kafka" 1 "$send_log" "" \
      "Replay did not send events cleanly." \
      "tail -n 120 '$send_log'"
  fi
fi

if ((SKIP_FLINK_SUBMIT == 1)); then
  p1_report WARN "flink_submit" "skipped by --skip-flink-submit"
else
  flink_log="$P1_LOG_DIR/flink_submit.log"
  mkdir -p "$(dirname "$flink_log")"
  printf '[START] flink_submit -> %s\n' "$flink_log"
  bash -lc "cd '$P1_PROJECT_ROOT' && export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink HIVE_CONF_DIR=/export/server/hive/conf METROPT_CONFIG='$CONFIG' && export PATH=/usr/local/bin:/usr/bin:/bin:\$JAVA_HOME/bin:\$FLINK_HOME/bin:\$PATH && timeout '$SUBMIT_TIMEOUT' '$FLINK_PYTHON' streaming/01_flink_metropt_kafka_to_hive.py --startup-mode '$STARTUP_MODE' --group-id '$GROUP_ID'" > "$flink_log" 2>&1
  flink_rc=$?
  if ((flink_rc == 0)) || grep -q 'MetroPT Flink 作业已提交' "$flink_log"; then
    p1_report PASS "flink_submit" "submit observed rc=$flink_rc"
  else
    p1_report FAIL "flink_submit" "rc=$flink_rc log=$flink_log"
    p1_failure_template "flink_submit" "Flink" "$flink_rc" "$flink_log" "" \
      "Flink streaming job submit failed." \
      "/export/server/flink/bin/flink list; tail -n 120 /export/server/flink/log/*"
  fi
fi

p1_run_logged "flink_running_jobs" "Flink" "$P1_LOG_DIR/flink_running_jobs.log" \
  "Flink CLI cannot list jobs or no JobManager is reachable." \
  "bin/p0_cluster_health_check.sh --module redis-flink; /export/server/flink/bin/flink list" \
  bash -lc "export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink; /export/server/flink/bin/flink list"

if ((WAIT_SECONDS > 0)); then
  printf '[INFO] wait up to %s seconds for Flink/Hive/Redis visibility\n' "$WAIT_SECONDS"
  wait_for_redis_pattern "$REDIS_PATTERN" "realtime KPI" "$WAIT_SECONDS"
fi

hive_sql="$P1_LOG_DIR/realtime_hive_check.sql"
cat > "$hive_sql" <<'SQL'
USE metropt_quality;
SHOW TABLES LIKE '*realtime*';
DESCRIBE FORMATTED ods_metropt_realtime_readings;
SHOW PARTITIONS ods_metropt_realtime_readings;
SELECT * FROM ods_metropt_realtime_readings LIMIT 5;
SELECT * FROM dws_metropt_realtime_kpi_1min LIMIT 10;
SELECT 'ods_metropt_realtime_readings', COUNT(*) FROM ods_metropt_realtime_readings;
SELECT 'dws_metropt_realtime_kpi_1min', COUNT(*) FROM dws_metropt_realtime_kpi_1min;
SQL

hive_log="$P1_LOG_DIR/realtime_hive_check.log"
p1_run_logged "realtime_hive_check_plain" "Hive" "$hive_log" \
  "Realtime Hive Beeline check failed." \
  "cd $P1_PROJECT_ROOT && bin/metropt_hive_mr_count_check.sh --mode realtime" \
  bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$hive_sql'"
hive_rc=$?

if ((hive_rc != 0)); then
  fallback_log="$P1_LOG_DIR/realtime_hive_count_fallback.log"
  p1_run_logged "realtime_hive_count_fallback" "Hive" "$fallback_log" \
    "Realtime Hive COUNT fallback failed." \
    "yarn logs -applicationId <application_id> -appOwner common" \
    bash -lc "cd '$P1_PROJECT_ROOT' && bin/metropt_hive_mr_count_check.sh --mode realtime"
fi

if grep -q 'ods_metropt_realtime_readings' "$hive_log"; then
  p1_report PASS "realtime_hive_visibility" "realtime Hive tables queried"
else
  p1_report FAIL "realtime_hive_visibility" "realtime table output missing in $hive_log"
  p1_failure_template "realtime_hive_visibility" "Hive" 1 "$hive_log" "$(p1_extract_application_id "$hive_log")" \
    "Hive realtime tables are not visible or query returned no expected table names." \
    "tail -n 120 '$hive_log'"
fi

redis_log="$P1_LOG_DIR/redis_kpi_check.log"
p1_run_logged "redis_kpi_check" "Redis" "$redis_log" \
  "Redis KPI keys are missing." \
  "redis-cli -h 192.168.88.101 -p 6379 --scan --pattern '$REDIS_PATTERN' | head" \
  bash -lc "KEYS=\$(redis-cli -h 192.168.88.101 -p 6379 --scan --pattern '$REDIS_PATTERN' | head -n 10); test -n \"\$KEYS\"; echo \"\$KEYS\"; FIRST=\$(printf '%s\n' \"\$KEYS\" | head -n 1); test -n \"\$FIRST\" && redis-cli -h 192.168.88.101 -p 6379 HGETALL \"\$FIRST\""

dlq_normal_log="$P1_LOG_DIR/dlq_normal_check.log"
timeout 15 bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server 192.168.88.101:9092 --topic '$DLQ_TOPIC' --max-messages 1 --timeout-ms 10000" > "$dlq_normal_log" 2>&1
dlq_normal_rc=$?
if [[ -s "$dlq_normal_log" ]] && grep -q '{' "$dlq_normal_log"; then
  p1_report FAIL "dlq_normal_check" "normal replay produced or exposed a DLQ message"
  p1_failure_template "dlq_normal_check" "Kafka/Flink" "$dlq_normal_rc" "$dlq_normal_log" "" \
    "DLQ should stay empty for normal replay validation." \
    "tail -n 80 '$dlq_normal_log'"
else
  p1_report PASS "dlq_normal_check" "no DLQ message observed for normal replay"
fi

if ((INJECT_DLQ_TEST == 1)); then
  inject_log="$P1_LOG_DIR/dlq_inject_check.log"
  {
    echo '{"event_id":null,"raw_index":null}'
    sleep 10
  } | bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-producer.sh --bootstrap-server 192.168.88.101:9092 --topic '$KAFKA_TOPIC'" > "$inject_log" 2>&1
  timeout 30 bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-console-consumer.sh --bootstrap-server 192.168.88.101:9092 --topic '$DLQ_TOPIC' --from-beginning --max-messages 1 --timeout-ms 20000" >> "$inject_log" 2>&1
  if grep -q '{' "$inject_log"; then
    p1_report PASS "dlq_inject_check" "DLQ message observed after bad event injection"
  else
    p1_report FAIL "dlq_inject_check" "no DLQ message observed after bad event injection"
    p1_failure_template "dlq_inject_check" "Kafka/Flink" 1 "$inject_log" "" \
      "Malformed event did not appear in DLQ." \
      "tail -n 120 '$inject_log'; /export/server/flink/bin/flink list"
  fi
else
  p1_report WARN "dlq_inject_check" "skipped; pass --inject-dlq-test to force bad event validation"
fi

p1_finish

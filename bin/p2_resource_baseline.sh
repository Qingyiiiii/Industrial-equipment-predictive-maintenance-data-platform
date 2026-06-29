#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
MODE="basic"
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p2_resource_baseline.sh [--mode basic|realtime|extended|all] [--hosts "hadoop1 hadoop2 hadoop3"]

Purpose:
  Read-only resource baseline capture. It never starts or stops services.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --hosts)
      read -r -a HOSTS <<< "${2:-}"
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

case "$MODE" in
  basic|realtime|extended|all) ;;
  *)
    echo "--mode must be basic, realtime, extended or all" >&2
    exit 2
    ;;
esac

p2_init "resource_baseline_${MODE}" "${ORIGINAL_ARGS[@]}"
p2_header "P2 resource baseline"
printf 'mode=%s\nhosts=%s\n\n' "$MODE" "${HOSTS[*]}"

modes_for_run() {
  if [[ "$MODE" == "all" ]]; then
    printf '%s\n' basic realtime extended
  else
    printf '%s\n' "$MODE"
  fi
}

capture_host_snapshot() {
  local mode="$1"
  local host="$2"
  local log="$P2_RUN_DIR/${mode}_${host}_system.log"
  local cmd
  cmd=$(cat <<'CMD'
set +e
echo "section=identity"
date '+%F %T'
hostname -f 2>/dev/null || hostname
whoami
echo
echo "section=load"
uptime
cat /proc/loadavg
echo
echo "section=memory"
free -m
echo
echo "section=disk"
df -h
echo
echo "section=jps"
jps -l 2>/dev/null || true
echo
echo "section=ports"
ss -lntp 2>/dev/null | egrep ':(8020|9870|8088|8042|5432|9083|10000|9092|9093|6379|8081|8080|18030|9030|18040|9050|9060|8060)[[:space:]]' || true
echo
echo "section=top_rss"
ps -eo pid,ppid,comm,%cpu,%mem,rss,args --sort=-rss 2>/dev/null | head -n 25 || true
echo
echo "section=log_sizes"
for d in /export/logs/hive /export/server/hadoop/logs /export/server/flink/log /export/logs/kafka /export/server/doris/fe/log /export/server/doris/be/log /export/data/trino/var/log /export/server/trino/var/log; do
  if [[ -d "$d" ]]; then
    du -sh "$d" 2>/dev/null
  fi
done
CMD
)
  if p2_run_on "$host" "$cmd" > "$log" 2>&1; then
    p2_report PASS "${mode}_${host}_system_snapshot" "log=$log"
  else
    p2_report FAIL "${mode}_${host}_system_snapshot" "host capture failed; log=$log"
  fi
}

capture_yarn_snapshot() {
  local mode="$1"
  local log="$P2_RUN_DIR/${mode}_hadoop1_yarn.log"
  local cmd
  cmd=$(cat <<'CMD'
set +e
export JAVA_HOME=/export/server/jdk17
export PATH=$JAVA_HOME/bin:/export/server/hadoop/bin:/export/server/hadoop/sbin:$PATH
echo "section=yarn_nodes"
yarn node -list -all 2>&1 || true
echo
echo "section=yarn_running_apps"
yarn application -list -appStates RUNNING 2>&1 || true
echo
echo "section=rm_metrics"
if command -v curl >/dev/null 2>&1; then
  curl -s http://hadoop1:8088/ws/v1/cluster/metrics || true
  echo
  curl -s http://hadoop1:8088/ws/v1/cluster/nodes || true
  echo
else
  echo "curl missing; REST metrics skipped"
fi
CMD
)
  if p2_run_on hadoop1 "$cmd" > "$log" 2>&1; then
    p2_report PASS "${mode}_yarn_snapshot" "log=$log"
  else
    p2_report WARN "${mode}_yarn_snapshot" "YARN capture returned non-zero; log=$log"
  fi
}

probe_basic_components() {
  p2_check_port hadoop1 8020 "HDFS RPC"
  p2_check_port hadoop1 8088 "YARN ResourceManager"
  p2_check_port hadoop1 5432 "PostgreSQL"
  p2_check_port hadoop1 9083 "Hive Metastore"
  p2_check_port hadoop1 10000 "HiveServer2"
  p2_check_port hadoop1 6379 "Redis"
  local host
  for host in "${HOSTS[@]}"; do
    p2_check_jps "$host" 'kafka.Kafka' "Kafka"
    p2_check_port "$host" 9092 "Kafka broker"
    p2_check_port "$host" 9093 "Kafka controller"
  done
  p2_check_port hadoop1 8081 "Flink JobManager"
}

probe_realtime_components() {
  probe_basic_components
  local redis_log="$P2_RUN_DIR/realtime_redis_kpi_keys.log"
  if p2_run_on hadoop1 "redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' 2>/dev/null | head -n 20" > "$redis_log" 2>&1; then
    if [[ -s "$redis_log" ]]; then
      p2_report PASS "realtime_redis_kpi_keys" "sample=$redis_log"
    else
      p2_report SKIP "realtime_redis_kpi_keys" "no metropt:kpi:1m:* keys observed"
    fi
  else
    p2_report WARN "realtime_redis_kpi_keys" "redis scan failed; log=$redis_log"
  fi
  local flink_log="$P2_RUN_DIR/realtime_flink_jobs.log"
  if p2_run_on hadoop1 "export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list 2>&1 || true" > "$flink_log" 2>&1; then
    p2_report PASS "realtime_flink_jobs" "log=$flink_log"
  else
    p2_report WARN "realtime_flink_jobs" "Flink list failed; log=$flink_log"
  fi
}

probe_extended_components() {
  p2_check_port hadoop1 8080 "Trino coordinator"
  p2_check_port hadoop1 18030 "Doris FE http"
  p2_check_port hadoop1 9030 "Doris FE mysql"
  local host
  for host in "${HOSTS[@]}"; do
    p2_check_port "$host" 18040 "Doris BE web"
    p2_check_port "$host" 9050 "Doris BE heartbeat"
    p2_check_port "$host" 9060 "Doris BE brpc"
    p2_check_port "$host" 8060 "Doris BE http"
  done
}

while IFS= read -r mode; do
  [[ -z "$mode" ]] && continue
  printf '\n[MODE] %s\n' "$mode"
  for host in "${HOSTS[@]}"; do
    capture_host_snapshot "$mode" "$host"
  done
  capture_yarn_snapshot "$mode"
  case "$mode" in
    basic) probe_basic_components ;;
    realtime) probe_realtime_components ;;
    extended) probe_extended_components ;;
  esac
done < <(modes_for_run)

p2_finish

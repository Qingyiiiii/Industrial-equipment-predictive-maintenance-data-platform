#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p7_ops_snapshot.sh [--hosts "hadoop1 hadoop2 hadoop3"]

Purpose:
  Read-only ops snapshot for process, port, resource, YARN, Kafka, Flink, Trino and Doris state.
  It never starts, stops, installs, or edits services.

Creates:
  ops_snapshot.md
  ops_snapshot.json
  host_metrics.tsv
  service_status.tsv
  readiness.tsv
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

p2_init "p7_ops_snapshot" "${ORIGINAL_ARGS[@]}"
p2_header "P7 ops snapshot"
printf 'hosts=%s\n\n' "${HOSTS[*]}"

HOST_METRICS="$P2_RUN_DIR/host_metrics.tsv"
SERVICE_STATUS="$P2_RUN_DIR/service_status.tsv"
READINESS_TSV="$P2_RUN_DIR/readiness.tsv"
NEXT_COMMANDS="$P2_RUN_DIR/next_commands.tsv"
SNAPSHOT_MD="$P2_RUN_DIR/ops_snapshot.md"
SNAPSHOT_JSON="$P2_RUN_DIR/ops_snapshot.json"

printf 'host\tstatus\tmem_total_mb\tmem_available_mb\tmem_available_pct\tload1\tcores\troot_used_pct\texport_used_pct\thome_used_pct\traw_log\tnext_command\n' > "$HOST_METRICS"
printf 'component\tstatus\tdetail\tnext_command\n' > "$SERVICE_STATUS"
printf 'mode\tstatus\treason\tnext_command\n' > "$READINESS_TSV"
printf 'area\tnext_command\n' > "$NEXT_COMMANDS"

HDFS_OK=0
YARN_OK=0
HIVE_OK=0
KAFKA_ENDPOINTS_OK=1
KAFKA_OK=0
REDIS_OK=0
FLINK_OK=0
TRINO_OK=0
DORIS_OK=0
MIN_MEM_AVAILABLE_MB=999999
MIN_MEM_AVAILABLE_PCT=100
HADOOP1_MEM_AVAILABLE_MB=0
YARN_RUNNING_NODES=0
DORIS_ALIVE_BACKENDS=0

clean_field() {
  local value="${1:-}"
  value="${value//$'\t'/ }"
  value="${value//$'\r'/ }"
  value="${value//$'\n'/ }"
  printf '%s' "$value"
}

add_service_status() {
  local component="$1"
  local status="$2"
  local detail="$3"
  local next_command="${4:-N/A}"
  detail="$(clean_field "$detail")"
  next_command="$(clean_field "$next_command")"
  printf '%s\t%s\t%s\t%s\n' "$component" "$status" "$detail" "$next_command" >> "$SERVICE_STATUS"
  case "$status" in
    PASS) p2_report PASS "service_${component}" "$detail" ;;
    WARN) p2_report WARN "service_${component}" "$detail; next=$next_command" ;;
    SKIP) p2_report SKIP "service_${component}" "$detail" ;;
    *) p2_report WARN "service_${component}" "$detail; next=$next_command" ;;
  esac
}

add_readiness() {
  local mode="$1"
  local status="$2"
  local reason="$3"
  local next_command="${4:-N/A}"
  reason="$(clean_field "$reason")"
  next_command="$(clean_field "$next_command")"
  printf '%s\t%s\t%s\t%s\n' "$mode" "$status" "$reason" "$next_command" >> "$READINESS_TSV"
  if [[ "$status" == "READY" ]]; then
    p2_report PASS "readiness_${mode}" "$reason"
  elif [[ "$status" == "NOT_RUNNING" ]]; then
    p2_report SKIP "readiness_${mode}" "$reason"
  else
    p2_report WARN "readiness_${mode}" "$reason; next=$next_command"
  fi
}

add_next_command() {
  local area="$1"
  local cmd="$2"
  printf '%s\t%s\n' "$area" "$(clean_field "$cmd")" >> "$NEXT_COMMANDS"
}

capture_host_metrics() {
  local host="$1"
  local raw="$P2_RUN_DIR/${host}_ops_raw.log"
  local cmd metric record rh status mem_total mem_avail mem_pct load1 cores root_pct export_pct home_pct
  cmd=$(cat <<'CMD'
set +e
short="$(hostname -s 2>/dev/null || hostname)"
mem_total="$(free -m | awk '/Mem:/ {print $2}')"
mem_available="$(free -m | awk '/Mem:/ {print $7}')"
if [[ -n "$mem_total" && "$mem_total" -gt 0 ]]; then
  mem_pct=$((mem_available * 100 / mem_total))
else
  mem_pct=0
fi
load1="$(awk '{print $1}' /proc/loadavg 2>/dev/null)"
cores="$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || echo 1)"
root_pct="$(df -P / 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')"
export_pct="$(df -P /export 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')"
home_pct="$(df -P /home 2>/dev/null | awk 'NR==2 {gsub("%","",$5); print $5}')"
printf 'HOST_METRIC\t%s\tOK\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
  "$short" "${mem_total:-0}" "${mem_available:-0}" "${mem_pct:-0}" "${load1:-0}" "${cores:-1}" "${root_pct:-NA}" "${export_pct:-NA}" "${home_pct:-NA}"
echo
echo "section=jps"
jps -l 2>/dev/null || true
echo
echo "section=ports"
ss -lntp 2>/dev/null | egrep ':(8020|9870|8088|8042|5432|9083|10000|9092|9093|6379|8081|8080|18030|9030|18040|9050|9060|8060)[[:space:]]' || true
echo
echo "section=memory"
free -m || true
echo
echo "section=disk"
df -h || true
echo
echo "section=top_rss"
ps -eo pid,ppid,comm,%cpu,%mem,rss,args --sort=-rss 2>/dev/null | head -n 20 || true
CMD
)
  if p2_run_on "$host" "$cmd" > "$raw" 2>&1; then
    metric="$(awk -F'\t' '$1=="HOST_METRIC" {print; exit}' "$raw")"
    if [[ -n "$metric" ]]; then
      IFS=$'\t' read -r record rh status mem_total mem_avail mem_pct load1 cores root_pct export_pct home_pct <<< "$metric"
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$host" "$status" "$mem_total" "$mem_avail" "$mem_pct" "$load1" "$cores" "${root_pct:-NA}" "${export_pct:-NA}" "${home_pct:-NA}" "$raw" "tail -n 80 '$raw'" >> "$HOST_METRICS"
      if [[ "$mem_avail" =~ ^[0-9]+$ ]]; then
        ((mem_avail < MIN_MEM_AVAILABLE_MB)) && MIN_MEM_AVAILABLE_MB="$mem_avail"
        [[ "$host" == "hadoop1" ]] && HADOOP1_MEM_AVAILABLE_MB="$mem_avail"
      fi
      if [[ "$mem_pct" =~ ^[0-9]+$ ]]; then
        ((mem_pct < MIN_MEM_AVAILABLE_PCT)) && MIN_MEM_AVAILABLE_PCT="$mem_pct"
      fi
      p2_report PASS "host_${host}_snapshot" "mem_available=${mem_avail}MB pct=${mem_pct}% load1=${load1} raw=$raw"
    else
      printf '%s\tWARN\t0\t0\t0\t0\t0\tNA\tNA\tNA\t%s\t%s\n' "$host" "$raw" "tail -n 120 '$raw'" >> "$HOST_METRICS"
      p2_report WARN "host_${host}_snapshot" "metric line missing; raw=$raw"
    fi
  else
    printf '%s\tWARN\t0\t0\t0\t0\t0\tNA\tNA\tNA\t%s\t%s\n' "$host" "$raw" "ssh common@$host 'hostname; free -m; jps -l; ss -lntp'" >> "$HOST_METRICS"
    p2_report WARN "host_${host}_snapshot" "host capture failed; raw=$raw"
  fi
}

check_port() {
  local host="$1"
  local port="$2"
  local component="$3"
  local required="${4:-optional}"
  local next_command="${5:-ss -lntp | grep $port}"
  local out status
  out="$(p2_run_on "$host" "ss -lntp 2>/dev/null | grep -E '[:.]$port[[:space:]]' | head -n 1 || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    add_service_status "$component" PASS "$host port $port listening: $out" "$next_command"
    return 0
  fi
  status="SKIP"
  [[ "$required" == "required" ]] && status="WARN"
  add_service_status "$component" "$status" "$host port $port not listening" "$next_command"
  return 1
}

check_jps() {
  local host="$1"
  local regex="$2"
  local component="$3"
  local required="${4:-optional}"
  local next_command="${5:-ssh common@$host 'jps -l'}"
  local out status
  out="$(p2_run_on "$host" "jps -l 2>/dev/null | grep -E '$regex' | head -n 1 || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    add_service_status "$component" PASS "$host process: $out" "$next_command"
    return 0
  fi
  status="SKIP"
  [[ "$required" == "required" ]] && status="WARN"
  add_service_status "$component" "$status" "$host process not found" "$next_command"
  return 1
}

capture_hdfs_yarn() {
  local nn_ok=0 rm_ok=0 hdfs_cmd yarn_log running_apps_log
  check_jps hadoop1 'org.apache.hadoop.hdfs.server.namenode.NameNode' 'hdfs_namenode' required "ssh common@hadoop1 'jps -l | grep NameNode; tail -n 120 /export/server/hadoop/logs/*namenode*.log'" && nn_ok=1
  check_port hadoop1 8020 'hdfs_rpc' required "ssh common@hadoop1 'ss -lntp | grep 8020; hdfs dfsadmin -report | head -n 40'" || true
  check_port hadoop1 9870 'hdfs_web' required "ssh common@hadoop1 'ss -lntp | grep 9870; curl -s http://hadoop1:9870 | head'" || true

  local host
  for host in "${HOSTS[@]}"; do
    check_jps "$host" 'org.apache.hadoop.hdfs.server.datanode.DataNode' "hdfs_datanode_${host}" required "ssh common@$host 'jps -l | grep DataNode; tail -n 120 /export/server/hadoop/logs/*datanode*.log'" || true
    check_jps "$host" 'org.apache.hadoop.yarn.server.nodemanager.NodeManager' "yarn_nodemanager_${host}" required "ssh common@$host 'jps -l | grep NodeManager; tail -n 120 /export/server/hadoop/logs/*nodemanager*.log'" || true
  done

  check_jps hadoop1 'org.apache.hadoop.yarn.server.resourcemanager.ResourceManager' 'yarn_resourcemanager' required "ssh common@hadoop1 'jps -l | grep ResourceManager; tail -n 120 /export/server/hadoop/logs/*resourcemanager*.log'" && rm_ok=1
  check_port hadoop1 8088 'yarn_rm_web' required "ssh common@hadoop1 'ss -lntp | grep 8088; yarn node -list -all'" || true

  hdfs_cmd="$P2_RUN_DIR/hdfs_filesystem_check.log"
  if p2_run_on hadoop1 "export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/hdfs dfs -ls / >/dev/null && echo ok" > "$hdfs_cmd" 2>&1; then
    add_service_status "hdfs_filesystem" PASS "hdfs dfs -ls / ok; log=$hdfs_cmd" "hdfs dfs -ls /"
  else
    add_service_status "hdfs_filesystem" WARN "hdfs filesystem command failed; log=$hdfs_cmd" "tail -n 120 '$hdfs_cmd'; hdfs dfsadmin -report"
  fi

  yarn_log="$P2_RUN_DIR/yarn_nodes.log"
  p2_run_on hadoop1 "export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/yarn node -list -states RUNNING 2>&1" > "$yarn_log" 2>&1 || true
  YARN_RUNNING_NODES="$(awk '/RUNNING/ {c++} END {print c+0}' "$yarn_log")"
  if [[ "$YARN_RUNNING_NODES" -ge 3 ]]; then
    add_service_status "yarn_running_nodes" PASS "running_nodes=$YARN_RUNNING_NODES log=$yarn_log" "yarn node -list -all"
  else
    add_service_status "yarn_running_nodes" WARN "running_nodes=$YARN_RUNNING_NODES expected>=3 log=$yarn_log" "yarn node -list -all; tail -n 120 /export/server/hadoop/logs/*nodemanager*.log"
  fi

  running_apps_log="$P2_RUN_DIR/yarn_running_apps.log"
  p2_run_on hadoop1 "export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/yarn application -list -appStates RUNNING 2>&1" > "$running_apps_log" 2>&1 || true
  add_service_status "yarn_running_apps" PASS "log=$running_apps_log" "yarn application -list -appStates RUNNING"

  if ((nn_ok == 1 && rm_ok == 1 && YARN_RUNNING_NODES >= 1)); then
    HDFS_OK=1
    YARN_OK=1
  fi
}

capture_hive() {
  local pg_ok=0 ms_ok=0 hs2_ok=0
  check_port hadoop1 5432 'postgresql' required "sudo systemctl status postgresql postgresql-15 --no-pager" && pg_ok=1
  check_jps hadoop1 'org.apache.hadoop.hive.metastore.HiveMetaStore' 'hive_metastore_process' required "tail -n 120 /export/logs/hive/hive-metastore.out" || true
  check_port hadoop1 9083 'hive_metastore_port' required "ss -lntp | grep 9083; tail -n 120 /export/logs/hive/hive-metastore.out" && ms_ok=1
  check_jps hadoop1 'org.apache.hive.service.server.HiveServer2' 'hiveserver2_process' required "tail -n 120 /export/logs/hive/hiveserver2.out" || true
  check_port hadoop1 10000 'hiveserver2_port' required "ss -lntp | grep 10000; tail -n 120 /export/logs/hive/hiveserver2.out" && hs2_ok=1
  if ((pg_ok == 1 && ms_ok == 1 && hs2_ok == 1)); then
    HIVE_OK=1
  fi
}

capture_kafka() {
  local host quorum_log topics_log
  for host in "${HOSTS[@]}"; do
    check_jps "$host" 'kafka.Kafka' "kafka_process_${host}" required "ssh common@$host 'export JAVA_HOME=/export/server/jdk17; jps -l | grep kafka; tail -n 120 /export/logs/kafka/kafka-server.out'" || KAFKA_ENDPOINTS_OK=0
    check_port "$host" 9092 "kafka_broker_${host}" required "ssh common@$host 'ss -lntp | grep 9092; tail -n 120 /export/logs/kafka/kafka-server.out'" || KAFKA_ENDPOINTS_OK=0
    check_port "$host" 9093 "kafka_controller_${host}" required "ssh common@$host 'ss -lntp | grep 9093; tail -n 120 /export/logs/kafka/kafka-server.out'" || KAFKA_ENDPOINTS_OK=0
  done

  quorum_log="$P2_RUN_DIR/kafka_quorum.log"
  p2_run_on hadoop1 "tmp=\$(mktemp); export JAVA_HOME=/export/server/jdk17; timeout 25 /export/server/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller hadoop1:9093 describe --status >\"\$tmp\" 2>&1; rc=\$?; cat \"\$tmp\"; echo __RC=\$rc; rm -f \"\$tmp\"" > "$quorum_log" 2>&1 || true
  if grep -q '__RC=0' "$quorum_log" && grep -q 'ClusterId' "$quorum_log" && grep -q 'LeaderId' "$quorum_log" && grep -q 'CurrentVoters' "$quorum_log"; then
    add_service_status "kafka_quorum" PASS "quorum ok log=$quorum_log" "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller hadoop1:9093 describe --status"
  else
    add_service_status "kafka_quorum" WARN "quorum check failed log=$quorum_log" "jps -l; ss -lntp | egrep '9092|9093'; tail -n 120 /export/logs/kafka/kafka-server.out"
  fi

  topics_log="$P2_RUN_DIR/kafka_topics.log"
  p2_run_on hadoop1 "tmp=\$(mktemp); export JAVA_HOME=/export/server/jdk17; timeout 25 /export/server/kafka/bin/kafka-topics.sh --bootstrap-server hadoop1:9092 --list >\"\$tmp\" 2>&1; rc=\$?; cat \"\$tmp\"; echo __RC=\$rc; rm -f \"\$tmp\"" > "$topics_log" 2>&1 || true
  if grep -q '__RC=0' "$topics_log" && ! grep -Eq 'Exception|ERROR|could not be established' "$topics_log"; then
    add_service_status "kafka_topics_cli" PASS "topics cli ok log=$topics_log" "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-topics.sh --bootstrap-server hadoop1:9092 --list"
  else
    add_service_status "kafka_topics_cli" WARN "topics cli failed log=$topics_log" "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-topics.sh --bootstrap-server hadoop1:9092 --list"
  fi

  if ((KAFKA_ENDPOINTS_OK == 1)) && grep -q '__RC=0' "$quorum_log"; then
    KAFKA_OK=1
  fi
}

capture_redis_flink() {
  local redis_log flink_log
  check_port hadoop1 6379 'redis_port' required "redis-cli -h hadoop1 ping" || true
  redis_log="$P2_RUN_DIR/redis_ping_and_keys.log"
  {
    redis-cli -h hadoop1 ping 2>&1 || true
    redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' 2>/dev/null | head -n 20 || true
  } > "$redis_log"
  if head -n 1 "$redis_log" | grep -q '^PONG$'; then
    REDIS_OK=1
    add_service_status "redis_ping" PASS "PONG log=$redis_log" "redis-cli -h hadoop1 ping; redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' | head"
  else
    add_service_status "redis_ping" WARN "redis ping failed log=$redis_log" "redis-cli -h hadoop1 ping; ss -lntp | grep 6379"
  fi

  check_port hadoop1 8081 'flink_web' required "export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list" && FLINK_OK=1
  flink_log="$P2_RUN_DIR/flink_jobs.log"
  p2_run_on hadoop1 "export JAVA_HOME=/export/server/jdk17 FLINK_HOME=/export/server/flink; /export/server/flink/bin/flink list 2>&1 || true" > "$flink_log" 2>&1 || true
  if grep -q 'No running jobs' "$flink_log"; then
    add_service_status "flink_jobs" PASS "Flink CLI ok; no running jobs; log=$flink_log" "export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list"
  elif grep -Eq '[0-9a-f]{32}' "$flink_log"; then
    add_service_status "flink_jobs" PASS "running or scheduled job observed; log=$flink_log" "export JAVA_HOME=/export/server/jdk17; /export/server/flink/bin/flink list"
  else
    add_service_status "flink_jobs" WARN "Flink list output did not show normal state; log=$flink_log" "tail -n 120 /export/server/flink/log/*; /export/server/flink/bin/flink list"
  fi
}

capture_trino() {
  local trino_log
  if check_port hadoop1 8080 'trino_coordinator' optional "bin/start_extended_query_mode.sh --trino-only"; then
    trino_log="$P2_RUN_DIR/trino_select1.log"
    p2_run_on hadoop1 "if command -v trino >/dev/null 2>&1; then timeout 45 trino --server http://hadoop1:8080 --execute 'SELECT 1'; elif [[ -x /export/server/trino/bin/trino ]]; then timeout 45 /export/server/trino/bin/trino --server http://hadoop1:8080 --execute 'SELECT 1'; else echo MISSING_CLI; fi" > "$trino_log" 2>&1 || true
    if grep -q '1' "$trino_log" && ! grep -Eq 'Exception|ERROR' "$trino_log"; then
      TRINO_OK=1
      add_service_status "trino_select1" PASS "SELECT 1 ok log=$trino_log" "bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke"
    elif grep -q 'MISSING_CLI' "$trino_log"; then
      add_service_status "trino_select1" SKIP "Trino CLI missing; port-only evidence log=$trino_log" "install or link Trino CLI, then run SELECT 1"
    else
      add_service_status "trino_select1" WARN "SELECT 1 failed log=$trino_log" "tail -n 120 /export/data/trino/var/log/server.log; /export/server/trino/bin/launcher status"
    fi
  fi
}

capture_doris() {
  local host be_log fe_log
  check_port hadoop1 18030 'doris_fe_http' optional "bin/start_extended_query_mode.sh --doris-only --allow-swapoff" || true
  if check_port hadoop1 9030 'doris_fe_mysql' optional "bin/start_extended_query_mode.sh --doris-only --allow-swapoff"; then
    fe_log="$P2_RUN_DIR/doris_frontends.tsv"
    if command -v mysql >/dev/null 2>&1 && mysql -h 127.0.0.1 -P 9030 -uroot --batch --raw -e 'SHOW FRONTENDS;' > "$fe_log" 2>&1; then
      add_service_status "doris_frontends" PASS "SHOW FRONTENDS ok log=$fe_log" "mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW FRONTENDS;'"
    else
      add_service_status "doris_frontends" WARN "SHOW FRONTENDS failed log=$fe_log" "tail -n 120 /export/server/doris/fe/log/fe.out"
    fi
    be_log="$P2_RUN_DIR/doris_backends.tsv"
    if command -v mysql >/dev/null 2>&1 && mysql -h 127.0.0.1 -P 9030 -uroot --batch --raw -e 'SHOW BACKENDS;' > "$be_log" 2>&1; then
      DORIS_ALIVE_BACKENDS="$(awk -F'\t' 'NR==1 {for (i=1;i<=NF;i++) if (tolower($i)=="alive") alive=i} NR>1 && alive && tolower($alive)=="true" {ok++} END{print ok+0}' "$be_log")"
      if [[ "$DORIS_ALIVE_BACKENDS" -ge 3 ]]; then
        DORIS_OK=1
        add_service_status "doris_backends" PASS "alive_backends=$DORIS_ALIVE_BACKENDS log=$be_log" "mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW BACKENDS;'"
      else
        add_service_status "doris_backends" WARN "alive_backends=$DORIS_ALIVE_BACKENDS expected>=3 log=$be_log" "tail -n 120 /export/server/doris/be/log/be.out; mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW BACKENDS;'"
      fi
    else
      add_service_status "doris_backends" WARN "SHOW BACKENDS failed log=$be_log" "tail -n 120 /export/server/doris/fe/log/fe.out"
    fi
  fi
  for host in "${HOSTS[@]}"; do
    check_port "$host" 18040 "doris_be_web_${host}" optional "ssh common@$host 'ss -lntp | grep 18040; tail -n 120 /export/server/doris/be/log/be.out'" || true
    check_port "$host" 9050 "doris_be_heartbeat_${host}" optional "ssh common@$host 'ss -lntp | grep 9050; tail -n 120 /export/server/doris/be/log/be.out'" || true
    check_port "$host" 9060 "doris_be_rpc_${host}" optional "ssh common@$host 'ss -lntp | grep 9060; tail -n 120 /export/server/doris/be/log/be.out'" || true
    check_port "$host" 8060 "doris_be_brpc_${host}" optional "ssh common@$host 'ss -lntp | grep 8060; tail -n 120 /export/server/doris/be/log/be.out'" || true
  done
}

calculate_readiness() {
  local resource_note="min_mem_available=${MIN_MEM_AVAILABLE_MB}MB min_mem_available_pct=${MIN_MEM_AVAILABLE_PCT}% hadoop1_mem_available=${HADOOP1_MEM_AVAILABLE_MB}MB"
  if ((HDFS_OK == 1 && YARN_OK == 1 && HIVE_OK == 1)); then
    if ((HADOOP1_MEM_AVAILABLE_MB >= 2048 && MIN_MEM_AVAILABLE_PCT >= 10)); then
      add_readiness "offline_hive_spark" READY "HDFS/YARN/Hive ready; $resource_note" "bin/p1_metropt_offline_acceptance.sh"
    else
      add_readiness "offline_hive_spark" WARN "Core services ready but resource headroom is low; $resource_note" "yarn application -list -appStates RUNNING; free -m; consider stopping Trino/Doris before offline run"
    fi
  else
    add_readiness "offline_hive_spark" NOT_READY "HDFS/YARN/Hive is incomplete; HDFS_OK=$HDFS_OK YARN_OK=$YARN_OK HIVE_OK=$HIVE_OK" "bin/start_base_services.sh; bin/p0_cluster_health_check.sh --module hdfs-yarn; bin/p0_cluster_health_check.sh --module hive"
  fi

  if ((HDFS_OK == 1 && YARN_OK == 1 && HIVE_OK == 1 && KAFKA_OK == 1 && REDIS_OK == 1 && FLINK_OK == 1)); then
    add_readiness "realtime_demo" READY "Kafka/Flink/Redis/Hive ready; $resource_note" "bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500"
  else
    add_readiness "realtime_demo" NOT_READY "Realtime dependencies incomplete; KAFKA_OK=$KAFKA_OK REDIS_OK=$REDIS_OK FLINK_OK=$FLINK_OK HIVE_OK=$HIVE_OK" "bin/start_realtime_mode.sh; bin/p0_cluster_health_check.sh --module kafka; bin/p0_cluster_health_check.sh --module redis-flink"
  fi

  if ((TRINO_OK == 1)); then
    add_readiness "trino_query" READY "Trino coordinator and SELECT 1 ready" "bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke"
  else
    add_readiness "trino_query" NOT_RUNNING "Trino not verified in this snapshot" "bin/start_extended_query_mode.sh --trino-only; bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke"
  fi

  if ((DORIS_OK == 1)); then
    add_readiness "doris_query" READY "Doris FE/BE ready; alive_backends=$DORIS_ALIVE_BACKENDS" "bin/p2_query_perf_compare.sh --engine hive,doris --query-set smoke"
  else
    add_readiness "doris_query" NOT_RUNNING "Doris not fully verified in this snapshot; alive_backends=$DORIS_ALIVE_BACKENDS" "bin/start_extended_query_mode.sh --doris-only --allow-swapoff; bin/p5_doris_acceptance.sh --check-only"
  fi

  add_next_command "offline_hive_spark" "bin/p1_metropt_offline_acceptance.sh"
  add_next_command "realtime_demo" "bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500"
  add_next_command "trino_query" "bin/start_extended_query_mode.sh --trino-only; bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke"
  add_next_command "doris_query" "bin/start_extended_query_mode.sh --doris-only --allow-swapoff; bin/p5_doris_acceptance.sh --check-only"
}

append_tsv_markdown() {
  local file="$1"
  local columns="$2"
  if [[ ! -f "$file" ]]; then
    printf 'Missing `%s`.\n\n' "$file"
    return
  fi
  awk -F'\t' -v columns="$columns" '
    BEGIN {
      n=split(columns, names, ",")
      printf "|"
      for (i=1; i<=n; i++) printf " %s |", names[i]
      printf "\n|"
      for (i=1; i<=n; i++) printf " --- |"
      printf "\n"
    }
    NR>1 {
      printf "|"
      for (i=1; i<=n; i++) {
        value=$i
        gsub(/\|/, "/", value)
        printf " %s |", value
      }
      printf "\n"
    }
  ' "$file"
  printf '\n'
}

write_outputs() {
  cat > "$SNAPSHOT_MD" <<EOF
# P7 Ops Snapshot

- run_id: $P2_RUN_ID
- generated_at: $(date '+%F %T')
- project_root: $P2_PROJECT_ROOT
- run_dir: $P2_RUN_DIR
- hosts: ${HOSTS[*]}

## Readiness

EOF
  append_tsv_markdown "$READINESS_TSV" "Mode,Status,Reason,Next Command" >> "$SNAPSHOT_MD"

  cat >> "$SNAPSHOT_MD" <<EOF
## Host Resources

EOF
  append_tsv_markdown "$HOST_METRICS" "Host,Status,Mem Total MB,Mem Available MB,Mem Available %,Load1,Cores,Root Used %,Export Used %,Home Used %,Raw Log,Next Command" >> "$SNAPSHOT_MD"

  cat >> "$SNAPSHOT_MD" <<EOF
## Service Status

EOF
  append_tsv_markdown "$SERVICE_STATUS" "Component,Status,Detail,Next Command" >> "$SNAPSHOT_MD"

  cat >> "$SNAPSHOT_MD" <<EOF
## Next Commands

EOF
  append_tsv_markdown "$NEXT_COMMANDS" "Area,Next Command" >> "$SNAPSHOT_MD"

  python3 - "$SNAPSHOT_JSON" "$P2_RUN_ID" "$P2_RUN_DIR" "$HOST_METRICS" "$SERVICE_STATUS" "$READINESS_TSV" "$NEXT_COMMANDS" <<'PY'
import csv
import json
import sys
from pathlib import Path

out, run_id, run_dir, host_metrics, service_status, readiness, next_commands = sys.argv[1:]

def read_tsv(path):
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))

payload = {
    "phase": "P7",
    "run_id": run_id,
    "run_dir": run_dir,
    "host_metrics": read_tsv(host_metrics),
    "service_status": read_tsv(service_status),
    "readiness": read_tsv(readiness),
    "next_commands": read_tsv(next_commands),
}
Path(out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
  p2_report PASS "ops_snapshot_md" "snapshot=$SNAPSHOT_MD"
  p2_report PASS "ops_snapshot_json" "snapshot=$SNAPSHOT_JSON"
}

for host in "${HOSTS[@]}"; do
  capture_host_metrics "$host"
done
capture_hdfs_yarn
capture_hive
capture_kafka
capture_redis_flink
capture_trino
capture_doris
calculate_readiness
write_outputs

printf '\nops_snapshot_md=%s\nops_snapshot_json=%s\nreadiness_tsv=%s\nservice_status_tsv=%s\nhost_metrics_tsv=%s\n' \
  "$SNAPSHOT_MD" "$SNAPSHOT_JSON" "$READINESS_TSV" "$SERVICE_STATUS" "$HOST_METRICS"
p2_finish

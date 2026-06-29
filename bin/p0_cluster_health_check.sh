#!/usr/bin/env bash
set -uo pipefail

HOSTS=(${P0_HOSTS:-hadoop1 hadoop2 hadoop3})
PROFILE="basic"
WITH_HIVE_COUNT=1
MODULES=()
PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  bin/p0_cluster_health_check.sh [--profile basic|full] [--module MODULE] [--hosts "hadoop1 hadoop2 hadoop3"] [--skip-hive-count]

Profiles:
  basic  Check HDFS, YARN, PostgreSQL, Hive, Kafka, Redis and Flink.
  full   basic + Trino and Doris port/process checks.

Modules:
  shell         Remote shell and load check.
  hdfs-yarn     HDFS and YARN check.
  hive          PostgreSQL, Hive Metastore, HiveServer2 and Hive COUNT smoke.
  kafka         Kafka broker/controller/quorum/topic check.
  redis-flink   Redis and Flink check.
  trino-doris   Trino and Doris check.

Environment overrides:
  P0_HOSTS    Space-separated host list, default: hadoop1 hadoop2 hadoop3
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --module)
      MODULES+=("${2:-}")
      shift 2
      ;;
    --hosts)
      read -r -a HOSTS <<< "${2:-}"
      shift 2
      ;;
    --skip-hive-count)
      WITH_HIVE_COUNT=0
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

if [[ "$PROFILE" != "basic" && "$PROFILE" != "full" ]]; then
  echo "--profile must be basic or full" >&2
  exit 2
fi

normalize_modules() {
  if ((${#MODULES[@]} > 0)); then
    return
  fi

  MODULES=(shell hdfs-yarn hive kafka redis-flink)
  if [[ "$PROFILE" == "full" ]]; then
    MODULES+=(trino-doris)
  fi
}

validate_modules() {
  local module
  for module in "${MODULES[@]}"; do
    case "$module" in
      shell|hdfs-yarn|hive|kafka|redis-flink|trino-doris) ;;
      basic)
        MODULES=(shell hdfs-yarn hive kafka redis-flink)
        ;;
      full|all)
        MODULES=(shell hdfs-yarn hive kafka redis-flink trino-doris)
        ;;
      *)
        echo "--module must be one of: shell, hdfs-yarn, hive, kafka, redis-flink, trino-doris, basic, full, all" >&2
        exit 2
        ;;
    esac
  done
}

LOCAL_SHORT="$(hostname -s 2>/dev/null || hostname)"
LOCAL_FQDN="$(hostname -f 2>/dev/null || hostname)"

is_local_host() {
  [[ "$1" == "$LOCAL_SHORT" || "$1" == "$LOCAL_FQDN" || "$1" == "localhost" ]]
}

run_on() {
  local host="$1"
  local cmd="$2"
  if is_local_host "$host"; then
    bash -lc "$cmd"
  else
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
      "$host" "bash -lc $(printf '%q' "$cmd")"
  fi
}

report() {
  local level="$1"
  local msg="$2"
  case "$level" in
    PASS) ((PASS_COUNT++));;
    WARN) ((WARN_COUNT++));;
    FAIL) ((FAIL_COUNT++));;
  esac
  printf '[%s] %s\n' "$level" "$msg"
}

check_remote_shell() {
  local host
  for host in "${HOSTS[@]}"; do
    local out
    if out="$(run_on "$host" 'printf "%s user=%s load=%s\n" "$(hostname -s)" "$(whoami)" "$(cut -d " " -f 1-3 /proc/loadavg)"' 2>&1)"; then
      report PASS "$host shell ok: $out"
    else
      report FAIL "$host shell failed: ${out//$'\n'/ }"
    fi
  done
}

check_jps_class() {
  local host="$1"
  local class_regex="$2"
  local desc="$3"
  local required="${4:-required}"
  local out
  out="$(run_on "$host" "jps -l 2>/dev/null | grep -E '$class_regex' || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    report PASS "$host $desc process ok: $(printf '%s' "$out" | head -n 1)"
    return 0
  elif [[ "$required" == "required" ]]; then
    report FAIL "$host $desc process missing"
    return 1
  else
    report WARN "$host $desc process missing"
    return 1
  fi
}

check_port() {
  local host="$1"
  local port="$2"
  local desc="$3"
  local required="${4:-required}"
  local out
  out="$(run_on "$host" "ss -lntp 2>/dev/null | grep -E '[:.]$port[[:space:]]' || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    report PASS "$host $desc port $port listening"
    return 0
  elif [[ "$required" == "required" ]]; then
    report FAIL "$host $desc port $port not listening"
    return 1
  else
    report WARN "$host $desc port $port not listening"
    return 1
  fi
}

check_hdfs_yarn() {
  check_jps_class hadoop1 'org.apache.hadoop.hdfs.server.namenode.NameNode' 'HDFS NameNode'
  check_port hadoop1 8020 'HDFS RPC'
  check_port hadoop1 9870 'HDFS Web'

  local host
  for host in "${HOSTS[@]}"; do
    check_jps_class "$host" 'org.apache.hadoop.hdfs.server.datanode.DataNode' 'HDFS DataNode'
    check_jps_class "$host" 'org.apache.hadoop.yarn.server.nodemanager.NodeManager' 'YARN NodeManager'
  done

  check_jps_class hadoop1 'org.apache.hadoop.yarn.server.resourcemanager.ResourceManager' 'YARN ResourceManager'
  check_port hadoop1 8088 'YARN ResourceManager Web'

  local hdfs_out yarn_out
  hdfs_out="$(run_on hadoop1 'export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/hdfs dfs -ls / >/dev/null && echo ok' 2>&1 || true)"
  if [[ "$hdfs_out" == *"ok"* ]]; then
    report PASS "HDFS filesystem command ok"
  else
    report FAIL "HDFS filesystem command failed: ${hdfs_out//$'\n'/ }"
  fi

  yarn_out="$(run_on hadoop1 'export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/yarn node -list -states RUNNING 2>/dev/null | awk "/RUNNING/ {c++} END {print c+0}"' 2>&1 || true)"
  if [[ "$yarn_out" =~ ^[0-9]+$ && "$yarn_out" -ge 1 ]]; then
    report PASS "YARN running nodes: $yarn_out"
  else
    report FAIL "YARN running node list failed: ${yarn_out//$'\n'/ }"
  fi
}

check_postgres_hive() {
  check_port hadoop1 5432 'PostgreSQL'
  check_jps_class hadoop1 'org.apache.hadoop.hive.metastore.HiveMetaStore' 'Hive Metastore'
  check_jps_class hadoop1 'org.apache.hive.service.server.HiveServer2' 'HiveServer2'
  check_port hadoop1 9083 'Hive Metastore'
  check_port hadoop1 10000 'HiveServer2'

  local out
  out="$(run_on hadoop1 'timeout 45 bash -lc '\''export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u "jdbc:hive2://hadoop1:10000/default" -n common --silent=true --showHeader=false --outputformat=tsv2 -e "SHOW DATABASES;"'\'' 2>&1 | tail -n 20' 2>&1 || true)"
  if [[ "$out" == *"default"* || "$out" == *"metropt_quality"* ]]; then
    report PASS "HiveServer2 beeline SHOW DATABASES ok"
  else
    report FAIL "HiveServer2 beeline SHOW DATABASES failed: ${out//$'\n'/ }"
  fi

  if ((WITH_HIVE_COUNT == 0)); then
    report WARN "Hive COUNT smoke skipped by --skip-hive-count"
    return
  fi

  out="$(run_on hadoop1 'timeout 180 bash -lc '\''export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u "jdbc:hive2://hadoop1:10000/default" -n common --silent=true --showHeader=false --outputformat=tsv2 -e "USE metropt_quality; SELECT COUNT(*) FROM ods_metropt_readings;"'\'' 2>&1 | tail -n 30' 2>&1 || true)"
  if [[ "$out" == *"1516948"* ]]; then
    report PASS "Hive COUNT smoke ok: ods_metropt_readings=1516948"
    return
  fi

  if [[ -x "$SCRIPT_DIR/metropt_hive_mr_count_check.sh" ]]; then
    local fallback
    fallback="$(run_on hadoop1 "cd '$SCRIPT_DIR/..' 2>/dev/null || cd /home/common/tmp/pycharm_Design; timeout 300 '$SCRIPT_DIR/metropt_hive_mr_count_check.sh' --mode offline 2>&1 | tail -n 30" 2>&1 || true)"
    if [[ "$fallback" == *"1516948"* ]]; then
      report WARN "Hive COUNT plain Beeline failed but fallback script passed; likely HiveServer2/config drift: ${out//$'\n'/ }"
    else
      report FAIL "Hive COUNT smoke failed; plain=${out//$'\n'/ } fallback=${fallback//$'\n'/ }"
    fi
  else
    report FAIL "Hive COUNT smoke failed and fallback script missing: ${out//$'\n'/ }"
  fi
}

check_kafka() {
  local host
  local endpoints_ok=1
  for host in "${HOSTS[@]}"; do
    check_jps_class "$host" 'kafka.Kafka' 'Kafka broker' || endpoints_ok=0
    check_port "$host" 9092 'Kafka broker' || endpoints_ok=0
    check_port "$host" 9093 'Kafka controller' || endpoints_ok=0
  done

  if ((endpoints_ok == 0)); then
    report FAIL "Kafka CLI checks skipped because one or more broker/controller endpoints are down"
    return
  fi

  local quorum topics
  quorum="$(run_on hadoop1 'tmp=$(mktemp); export JAVA_HOME=/export/server/jdk17; timeout 20 /export/server/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller hadoop1:9093 describe --status >"$tmp" 2>&1; rc=$?; tail -n 10 "$tmp"; echo "__RC=$rc"; rm -f "$tmp"' 2>&1 || true)"
  if [[ "$quorum" == *"__RC=0"* && "$quorum" == *"ClusterId"* && "$quorum" == *"LeaderId"* && "$quorum" == *"CurrentVoters"* ]]; then
    report PASS "Kafka quorum status ok"
  else
    report FAIL "Kafka quorum status failed: ${quorum//$'\n'/ }"
  fi

  topics="$(run_on hadoop1 'tmp=$(mktemp); export JAVA_HOME=/export/server/jdk17; timeout 20 /export/server/kafka/bin/kafka-topics.sh --bootstrap-server hadoop1:9092 --list >"$tmp" 2>&1; rc=$?; tail -n 10 "$tmp"; echo "__RC=$rc"; rm -f "$tmp"' 2>&1 || true)"
  if [[ "$topics" == *"__RC=0"* && "$topics" != *"Exception"* && "$topics" != *"ERROR"* && "$topics" != *"WARN"* && "$topics" != *"could not be established"* ]]; then
    report PASS "Kafka topics CLI ok"
  else
    report FAIL "Kafka topics CLI failed: ${topics//$'\n'/ }"
  fi
}

check_redis_flink() {
  check_port hadoop1 6379 'Redis'
  local redis
  redis="$(run_on hadoop1 'redis-cli -h hadoop1 ping 2>/dev/null || true' 2>&1 || true)"
  if [[ "$redis" == "PONG" ]]; then
    report PASS "Redis ping ok"
  else
    report FAIL "Redis ping failed: ${redis//$'\n'/ }"
  fi

  check_port hadoop1 8081 'Flink Web'
  check_jps_class hadoop1 'org.apache.flink.runtime.entrypoint.*SessionClusterEntrypoint|StandaloneSessionClusterEntrypoint' 'Flink JobManager' optional
  local host
  for host in "${HOSTS[@]}"; do
    check_jps_class "$host" 'org.apache.flink.runtime.taskexecutor.TaskManagerRunner|TaskManagerRunner' 'Flink TaskManager' optional
  done
}

check_trino_doris() {
  local trino_port_ok=1
  check_port hadoop1 8080 'Trino coordinator' optional || trino_port_ok=0
  local trino
  if ((trino_port_ok == 0)); then
    report WARN "Trino SELECT 1 skipped because coordinator port is down"
  else
    trino="$(run_on hadoop1 'if command -v trino >/dev/null 2>&1; then timeout 45 trino --server http://hadoop1:8080 --execute "SELECT 1"; elif [[ -x /export/server/trino/bin/trino ]]; then timeout 45 /export/server/trino/bin/trino --server http://hadoop1:8080 --execute "SELECT 1"; else echo MISSING_CLI; fi' 2>&1 || true)"
    if [[ "$trino" == *'"_col0"'* || "$trino" == *"1"* && "$trino" != *"Exception"* ]]; then
      report PASS "Trino SELECT 1 ok"
    elif [[ "$trino" == *"MISSING_CLI"* ]]; then
      report WARN "Trino CLI missing; port-only check used"
    else
      report WARN "Trino SELECT 1 failed: ${trino//$'\n'/ }"
    fi
  fi

  check_port hadoop1 18030 'Doris FE web' optional
  check_port hadoop1 9030 'Doris FE MySQL' optional
  local host
  for host in "${HOSTS[@]}"; do
    check_port "$host" 18040 'Doris BE web' optional
    check_port "$host" 9050 'Doris BE heartbeat' optional
    check_port "$host" 9060 'Doris BE brpc' optional
    check_port "$host" 8060 'Doris BE webserver' optional
  done
}

main() {
  normalize_modules
  validate_modules

  printf 'P0 cluster health check started at %s\n' "$(date '+%F %T')"
  printf 'Profile: %s\n' "$PROFILE"
  printf 'Hosts: %s\n\n' "${HOSTS[*]}"
  printf 'Modules: %s\n\n' "${MODULES[*]}"

  local module
  for module in "${MODULES[@]}"; do
    case "$module" in
      shell)
        check_remote_shell
        ;;
      hdfs-yarn)
        check_hdfs_yarn
        ;;
      hive)
        check_postgres_hive
        ;;
      kafka)
        check_kafka
        ;;
      redis-flink)
        check_redis_flink
        ;;
      trino-doris)
        check_trino_doris
        ;;
    esac
  done

  printf '\nSUMMARY pass=%d warn=%d fail=%d\n' "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT"
  if ((FAIL_COUNT > 0)); then
    exit 1
  fi
}

main "$@"

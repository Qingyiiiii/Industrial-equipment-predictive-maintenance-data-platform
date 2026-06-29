#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
DO_START=0
CHECK_ONLY=0
DO_LOAD=0
DO_QUERY=0
ALLOW_SWAPOFF=0
HIVE_DB="metropt_quality"
DORIS_DB="metropt_quality_olap"
EXPECTED_SENSOR_KPI_ROWS=15
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p5_doris_acceptance.sh [--start] [--check-only] [--load-kpi] [--query-smoke] [--allow-swapoff] [--hosts "hadoop1 hadoop2 hadoop3"]

Defaults:
  Validate Doris prerequisites and current health only. Use --start --load-kpi --query-smoke for the full P5 closure.

Notes:
  Doris is an extended-mode component. This script does not stop Trino/Flink/Kafka automatically.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start)
      DO_START=1
      shift
      ;;
    --check-only)
      CHECK_ONLY=1
      shift
      ;;
    --load-kpi)
      DO_LOAD=1
      shift
      ;;
    --query-smoke)
      DO_QUERY=1
      shift
      ;;
    --allow-swapoff)
      ALLOW_SWAPOFF=1
      shift
      ;;
    --hosts)
      read -r -a HOSTS <<< "${2:-}"
      shift 2
      ;;
    --hive-db)
      HIVE_DB="${2:-}"
      shift 2
      ;;
    --doris-db)
      DORIS_DB="${2:-}"
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

p2_init "p5_doris_acceptance" "${ORIGINAL_ARGS[@]}"
p2_header "P5 Doris extended query acceptance"
printf 'start=%s\ncheck_only=%s\nload_kpi=%s\nquery_smoke=%s\nallow_swapoff=%s\nhosts=%s\nhive_db=%s\ndoris_db=%s\n\n' \
  "$DO_START" "$CHECK_ONLY" "$DO_LOAD" "$DO_QUERY" "$ALLOW_SWAPOFF" "${HOSTS[*]}" "$HIVE_DB" "$DORIS_DB"

MYSQL="mysql -h 127.0.0.1 -P 9030 -uroot --batch --raw"

required_tool() {
  local tool="$1"
  if command -v "$tool" >/dev/null 2>&1; then
    p2_report PASS "tool:$tool" "$(command -v "$tool")"
  else
    p2_report FAIL "tool:$tool" "missing"
    return 1
  fi
}

last_conf_value() {
  local host="$1"
  local file="$2"
  local key="$3"
  p2_run_on "$host" "awk -F= -v key='$key' '\$1 ~ \"^[[:space:]]*\" key \"[[:space:]]*$\" {gsub(/^[[:space:]]+|[[:space:]]+$/, \"\", \$2); value=\$2} END {print value}' '$file' 2>/dev/null" 2>/dev/null || true
}

check_conf_value() {
  local host="$1"
  local file="$2"
  local key="$3"
  local expected="$4"
  local actual
  actual="$(last_conf_value "$host" "$file" "$key")"
  if [[ "$actual" == "$expected" ]]; then
    p2_report PASS "$host:$key" "$actual"
  else
    p2_report FAIL "$host:$key" "expected=$expected actual=${actual:-MISSING}"
  fi
}

check_prerequisites() {
  local host
  required_tool mysql || true
  for host in "${HOSTS[@]}"; do
    local swap_out ulimit_out libjvm_out node_port_owner
    swap_out="$(p2_run_on "$host" "swapon --show | awk 'NR>1 {print}'" 2>&1 || true)"
    if [[ -z "$swap_out" ]]; then
      p2_report PASS "$host:swap" "off"
    elif ((ALLOW_SWAPOFF == 1)); then
      p2_report WARN "$host:swap" "on; --allow-swapoff will be delegated to start script"
    else
      p2_report FAIL "$host:swap" "swap is on; rerun with --allow-swapoff after review"
    fi

    ulimit_out="$(p2_run_on "$host" "ulimit -n" 2>&1 || true)"
    if [[ "$ulimit_out" =~ ^[0-9]+$ && "$ulimit_out" -ge 655350 ]]; then
      p2_report PASS "$host:nofile" "$ulimit_out"
    else
      p2_report FAIL "$host:nofile" "expected>=655350 actual=${ulimit_out:-UNKNOWN}"
    fi

    libjvm_out="$(p2_run_on "$host" "if [[ -f /export/server/jdk17/lib/server/libjvm.so || -f /usr/lib/jvm/java-17-openjdk/lib/server/libjvm.so || -f /usr/lib64/jvm/java-17-openjdk/lib/server/libjvm.so ]]; then echo found; else find /export/server/jdk17 /usr/lib/jvm -path '*/lib/server/libjvm.so' -print -quit 2>/dev/null; fi" 2>&1 || true)"
    if [[ -n "$libjvm_out" ]]; then
      p2_report PASS "$host:libjvm" "$(printf '%s' "$libjvm_out" | head -n 1)"
    else
      p2_report FAIL "$host:libjvm" "JDK17 lib/server/libjvm.so not found"
    fi

    node_port_owner="$(p2_run_on "$host" "ss -lntp 2>/dev/null | grep -E '[:.]8040[[:space:]]' || true" 2>&1 || true)"
    if [[ -n "$node_port_owner" ]]; then
      p2_report PASS "$host:8040_owner" "8040 occupied by non-Doris service as expected in this cluster"
    fi

    if [[ "$host" == "hadoop1" ]]; then
      check_conf_value "$host" /export/server/doris/fe/conf/fe.conf http_port 18030
      check_conf_value "$host" /export/server/doris/fe/conf/fe.conf query_port 9030
      check_conf_value "$host" /export/server/doris/fe/conf/fe.conf edit_log_port 9010
      check_conf_value "$host" /export/server/doris/fe/conf/fe.conf rpc_port 9020
    fi
    check_conf_value "$host" /export/server/doris/be/conf/be.conf webserver_port 18040
    check_conf_value "$host" /export/server/doris/be/conf/be.conf heartbeat_service_port 9050
    check_conf_value "$host" /export/server/doris/be/conf/be.conf be_port 9060
    check_conf_value "$host" /export/server/doris/be/conf/be.conf brpc_port 8060
  done
}

start_doris_if_requested() {
  if ((CHECK_ONLY == 1 || DO_START == 0)); then
    p2_report SKIP "start_doris" "not requested"
    return 0
  fi
  local args=(--doris-only)
  if ((ALLOW_SWAPOFF == 1)); then
    args+=(--allow-swapoff)
  fi
  p2_run_logged "start_doris_extended_mode" "Doris" "$P2_RUN_DIR/start_doris_extended_mode.log" \
    "Doris start script failed. Inspect FE/BE logs and port ownership." \
    "cd $P2_PROJECT_ROOT && bin/start_extended_query_mode.sh --doris-only --allow-swapoff" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_extended_query_mode.sh ${args[*]}"
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local desc="$3"
  local max="${4:-30}"
  local i
  for i in $(seq 1 "$max"); do
    if p2_run_on "$host" "ss -lntp 2>/dev/null | grep -qE '[:.]$port[[:space:]]'" >/dev/null 2>&1; then
      p2_report PASS "$host:$desc" "port $port listening"
      return 0
    fi
    sleep 2
  done
  p2_report FAIL "$host:$desc" "port $port not listening"
  return 1
}

validate_ports() {
  local host
  if ((CHECK_ONLY == 1 || DO_START == 0)); then
    p2_check_port hadoop1 18030 "Doris FE http"
    p2_check_port hadoop1 9030 "Doris FE mysql"
    for host in "${HOSTS[@]}"; do
      p2_check_port "$host" 18040 "Doris BE web"
      p2_check_port "$host" 9050 "Doris BE heartbeat"
      p2_check_port "$host" 9060 "Doris BE be_port"
      p2_check_port "$host" 8060 "Doris BE brpc"
    done
    return 0
  fi

  wait_for_port hadoop1 18030 "Doris FE http" 30 || true
  wait_for_port hadoop1 9030 "Doris FE mysql" 30 || true
  for host in "${HOSTS[@]}"; do
    wait_for_port "$host" 18040 "Doris BE web" 30 || true
    wait_for_port "$host" 9050 "Doris BE heartbeat" 30 || true
    wait_for_port "$host" 9060 "Doris BE be_port" 30 || true
    wait_for_port "$host" 8060 "Doris BE brpc" 30 || true
  done
}

mysql_query() {
  local sql="$1"
  bash -lc "$MYSQL -e $(printf '%q' "$sql")"
}

validate_frontends_backends() {
  local fe_log="$P2_RUN_DIR/show_frontends.tsv"
  local be_log="$P2_RUN_DIR/show_backends.tsv"
  local i alive_count

  if ! p2_run_on hadoop1 "ss -lntp 2>/dev/null | grep -qE '[:.]9030[[:space:]]'" >/dev/null 2>&1; then
    if ((CHECK_ONLY == 1 || DO_START == 0)); then
      p2_report SKIP "show_frontends" "Doris FE mysql port 9030 is not running"
      p2_report SKIP "show_backends" "Doris FE mysql port 9030 is not running"
    else
      p2_report FAIL "show_frontends" "Doris FE mysql port 9030 is not running"
      p2_report FAIL "show_backends" "Doris FE mysql port 9030 is not running"
    fi
    return 0
  fi

  for i in $(seq 1 30); do
    if mysql_query "SHOW FRONTENDS;" > "$fe_log" 2>&1; then
      if awk -F'\t' 'NR==1 {for (i=1;i<=NF;i++) if (tolower($i)=="alive") alive=i} NR>1 && alive && tolower($alive)=="true" {ok++} END{exit ok>=1?0:1}' "$fe_log"; then
        p2_report PASS "show_frontends" "alive frontend found log=$fe_log"
        break
      fi
    fi
    if ((i == 30)); then
      p2_report FAIL "show_frontends" "no alive frontend found log=$fe_log"
      break
    fi
    sleep 2
  done

  for i in $(seq 1 45); do
    if mysql_query "SHOW BACKENDS;" > "$be_log" 2>&1; then
      alive_count="$(awk -F'\t' 'NR==1 {for (i=1;i<=NF;i++) if (tolower($i)=="alive") alive=i} NR>1 && alive && tolower($alive)=="true" {ok++} END{print ok+0}' "$be_log")"
    else
      alive_count=0
    fi
    if [[ "$alive_count" -ge 3 ]]; then
      p2_report PASS "show_backends" "alive_backends=$alive_count log=$be_log"
      break
    fi
    if ((i == 45)); then
      p2_report FAIL "show_backends" "expected_alive_backends>=3 actual=$alive_count log=$be_log"
      break
    fi
    sleep 2
  done
}

write_doris_ddl() {
  cat > "$P2_RUN_DIR/doris_sensor_kpi_ddl.sql" <<SQL
CREATE DATABASE IF NOT EXISTS $DORIS_DB;
USE $DORIS_DB;
CREATE TABLE IF NOT EXISTS dws_metropt_sensor_kpi (
  sensor_name VARCHAR(64) NOT NULL,
  sensor_type VARCHAR(64),
  station_id VARCHAR(128),
  unit VARCHAR(64),
  sample_count BIGINT,
  failure_sample_count BIGINT,
  avg_sensor_value DOUBLE,
  std_sensor_value DOUBLE,
  min_sensor_value DOUBLE,
  max_sensor_value DOUBLE,
  failure_window_rate DOUBLE
)
DUPLICATE KEY(sensor_name)
DISTRIBUTED BY HASH(sensor_name) BUCKETS 3
PROPERTIES (
  "replication_num" = "1"
);
TRUNCATE TABLE dws_metropt_sensor_kpi;
SQL
}

export_hive_sensor_kpi() {
  local out="$P2_RUN_DIR/dws_metropt_sensor_kpi.tsv"
  local log="$P2_RUN_DIR/export_hive_sensor_kpi.log"
  p2_run_logged "export_hive_sensor_kpi" "Hive" "$log" \
    "Hive export for Doris load failed." \
    "tail -n 120 '$log'" \
    bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=false --outputformat=tsv2 -e \"USE $HIVE_DB; SELECT sensor_name, sensor_type, station_id, unit, sample_count, failure_sample_count, avg_sensor_value, std_sensor_value, min_sensor_value, max_sensor_value, failure_window_rate FROM dws_metropt_sensor_kpi ORDER BY sensor_name\" > '$out'"
}

load_sensor_kpi_to_doris() {
  if ((CHECK_ONLY == 1 || DO_LOAD == 0)); then
    p2_report SKIP "load_doris_sensor_kpi" "not requested"
    return 0
  fi
  write_doris_ddl
  export_hive_sensor_kpi

  local load_sql="$P2_RUN_DIR/doris_sensor_kpi_load.sql"
  cat > "$load_sql" <<SQL
USE $DORIS_DB;
LOAD DATA LOCAL INFILE '$P2_RUN_DIR/dws_metropt_sensor_kpi.tsv'
INTO TABLE dws_metropt_sensor_kpi
COLUMNS TERMINATED BY '\t'
(sensor_name, sensor_type, station_id, unit, sample_count, failure_sample_count, avg_sensor_value, std_sensor_value, min_sensor_value, max_sensor_value, failure_window_rate);
SQL

  p2_run_logged "doris_create_sensor_kpi_table" "Doris" "$P2_RUN_DIR/doris_create_sensor_kpi_table.log" \
    "Doris DDL failed." \
    "tail -n 120 '$P2_RUN_DIR/doris_create_sensor_kpi_table.log'" \
    bash -lc "$MYSQL < '$P2_RUN_DIR/doris_sensor_kpi_ddl.sql'"

  p2_run_logged "doris_load_sensor_kpi" "Doris" "$P2_RUN_DIR/doris_load_sensor_kpi.log" \
    "Doris LOAD DATA failed." \
    "tail -n 120 '$P2_RUN_DIR/doris_load_sensor_kpi.log'" \
    bash -lc "$MYSQL --local-infile=1 < '$load_sql'"

  local count_log="$P2_RUN_DIR/doris_sensor_kpi_count.tsv"
  if mysql_query "USE $DORIS_DB; SELECT COUNT(*) FROM dws_metropt_sensor_kpi;" > "$count_log" 2>&1; then
    local actual
    actual="$(tail -n 1 "$count_log" | tr -d '[:space:]')"
    if [[ "$actual" == "$EXPECTED_SENSOR_KPI_ROWS" ]]; then
      p2_report PASS "doris_sensor_kpi_count" "actual=$actual expected=$EXPECTED_SENSOR_KPI_ROWS log=$count_log"
    else
      p2_report FAIL "doris_sensor_kpi_count" "actual=${actual:-UNKNOWN} expected=$EXPECTED_SENSOR_KPI_ROWS log=$count_log"
    fi
  else
    p2_report FAIL "doris_sensor_kpi_count" "query failed log=$count_log"
  fi
}

run_query_smoke() {
  if ((CHECK_ONLY == 1 || DO_QUERY == 0)); then
    p2_report SKIP "query_smoke" "not requested"
    return 0
  fi
  local engines="hive,doris"
  if p2_run_on hadoop1 "ss -lntp 2>/dev/null | grep -qE '[:.]8080[[:space:]]'" >/dev/null 2>&1; then
    engines="hive,trino,doris"
  fi
  p2_run_logged "query_perf_compare_${engines//,/_}" "Query" "$P2_RUN_DIR/query_perf_compare.log" \
    "Hive/Trino/Doris smoke comparison failed." \
    "cd $P2_PROJECT_ROOT && bin/p2_query_perf_compare.sh --engine '$engines' --query-set smoke --doris-db '$DORIS_DB'" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p2_query_perf_compare.sh --engine '$engines' --query-set smoke --doris-db '$DORIS_DB'"
}

check_prerequisites
start_doris_if_requested
validate_ports
validate_frontends_backends
load_sensor_kpi_to_doris
run_query_smoke

p2_finish

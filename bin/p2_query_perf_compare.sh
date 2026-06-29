#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

ENGINES="hive"
QUERY_SET="smoke"
TIMEOUT_SECONDS=300
HIVE_DB="metropt_quality"
TRINO_SCHEMA="iceberg.metropt_quality_iceberg"
DORIS_DB="metropt_quality_olap"
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p2_query_perf_compare.sh [--engine hive,trino,doris] [--query-set smoke|baseline] [--timeout SECONDS]

Defaults:
  --engine hive --query-set smoke --timeout 300

Notes:
  Trino and Doris are never started by this script. If selected but not running, they are marked SKIP.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine|--engines)
      ENGINES="${2:-}"
      shift 2
      ;;
    --query-set)
      QUERY_SET="${2:-}"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --hive-db)
      HIVE_DB="${2:-}"
      shift 2
      ;;
    --trino-schema)
      TRINO_SCHEMA="${2:-}"
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

case "$QUERY_SET" in
  smoke|baseline) ;;
  *)
    echo "--query-set must be smoke or baseline" >&2
    exit 2
    ;;
esac
if [[ "$TIMEOUT_SECONDS" =~ [^0-9] || -z "$TIMEOUT_SECONDS" ]]; then
  echo "--timeout must be a non-negative integer" >&2
  exit 2
fi

p2_init "query_perf_compare" "${ORIGINAL_ARGS[@]}"
p2_header "P2 query performance compare"
printf 'engines=%s\nquery_set=%s\ntimeout=%s\n\n' "$ENGINES" "$QUERY_SET" "$TIMEOUT_SECONDS"

RESULTS="$P2_RUN_DIR/query_perf_results.tsv"
printf 'engine\tquery_id\treturn_code\treal_seconds\tlog\n' > "$RESULTS"

write_sql_files() {
  mkdir -p "$P2_RUN_DIR/sql"
  cat > "$P2_RUN_DIR/sql/hive_01_ods_count.sql" <<SQL
USE $HIVE_DB;
SELECT 'ods_metropt_readings' AS table_name, COUNT(*) AS row_count FROM ods_metropt_readings;
SQL
  cat > "$P2_RUN_DIR/sql/hive_02_sensor_kpi_limit.sql" <<SQL
USE $HIVE_DB;
SELECT * FROM dws_metropt_sensor_kpi LIMIT 5;
SQL
  cat > "$P2_RUN_DIR/sql/trino_01_ods_count.sql" <<SQL
SELECT 'ods_metropt_readings' AS table_name, COUNT(*) AS row_count FROM $TRINO_SCHEMA.ods_metropt_readings;
SQL
  cat > "$P2_RUN_DIR/sql/trino_02_sensor_kpi_limit.sql" <<SQL
SELECT * FROM $TRINO_SCHEMA.dws_metropt_sensor_kpi LIMIT 5;
SQL
  cat > "$P2_RUN_DIR/sql/doris_01_show_tables.sql" <<SQL
USE $DORIS_DB;
SHOW TABLES;
SQL
  cat > "$P2_RUN_DIR/sql/doris_02_sensor_kpi_count.sql" <<SQL
USE $DORIS_DB;
SELECT 'dws_metropt_sensor_kpi' AS table_name, COUNT(*) AS row_count FROM dws_metropt_sensor_kpi;
SQL
  cat > "$P2_RUN_DIR/sql/doris_03_sensor_kpi_limit.sql" <<SQL
USE $DORIS_DB;
SELECT * FROM dws_metropt_sensor_kpi LIMIT 5;
SQL
  if [[ "$QUERY_SET" == "baseline" ]]; then
    cat > "$P2_RUN_DIR/sql/hive_03_core_counts.sql" <<SQL
USE $HIVE_DB;
SELECT 'dwd_metropt_sensor_long', COUNT(*) FROM dwd_metropt_sensor_long;
SELECT 'dws_metropt_window_kpi', COUNT(*) FROM dws_metropt_window_kpi;
SELECT 'dws_metropt_sensor_kpi', COUNT(*) FROM dws_metropt_sensor_kpi;
SQL
    cat > "$P2_RUN_DIR/sql/trino_03_core_counts.sql" <<SQL
SELECT 'dwd_metropt_sensor_long', COUNT(*) FROM $TRINO_SCHEMA.dwd_metropt_sensor_long;
SELECT 'dws_metropt_window_kpi', COUNT(*) FROM $TRINO_SCHEMA.dws_metropt_window_kpi;
SELECT 'dws_metropt_sensor_kpi', COUNT(*) FROM $TRINO_SCHEMA.dws_metropt_sensor_kpi;
SQL
  fi
}

engine_selected() {
  local engine="$1"
  [[ ",$ENGINES," == *",$engine,"* ]]
}

record_result() {
  local engine="$1"
  local query_id="$2"
  local rc="$3"
  local log="$4"
  local real
  real="$(awk '/^real / {print $2}' "$log" 2>/dev/null | tail -n 1)"
  printf '%s\t%s\t%s\t%s\t%s\n' "$engine" "$query_id" "$rc" "${real:-NA}" "$log" >> "$RESULTS"
}

run_timed() {
  local engine="$1"
  local query_id="$2"
  local sql_file="$3"
  local command="$4"
  local log="$P2_RUN_DIR/${engine}_${query_id}.log"
  (
    printf 'engine=%s\nquery_id=%s\nsql_file=%s\nstart=%s\n\n' "$engine" "$query_id" "$sql_file" "$(date '+%F %T')"
    if command -v /usr/bin/time >/dev/null 2>&1; then
      /usr/bin/time -p timeout "$TIMEOUT_SECONDS" bash -lc "$command"
    else
      timeout "$TIMEOUT_SECONDS" bash -lc "$command"
    fi
    rc=$?
    printf '\nend=%s\nreturn_code=%s\n' "$(date '+%F %T')" "$rc"
    exit "$rc"
  ) > "$log" 2>&1
  local rc=$?
  record_result "$engine" "$query_id" "$rc" "$log"
  if ((rc == 0)); then
    p2_report PASS "${engine}_${query_id}" "log=$log"
  else
    p2_report FAIL "${engine}_${query_id}" "rc=$rc log=$log"
    p2_failure_template "${engine}_${query_id}" "$engine" "$rc" "$log" "$(p2_extract_application_id "$log")" \
      "$engine query failed or timed out." \
      "tail -n 120 '$log'"
  fi
}

run_hive() {
  if ! p2_check_port hadoop1 10000 "HiveServer2" optional; then
    p2_report SKIP "hive_perf" "HiveServer2 is not running"
    return 0
  fi
  local file
  for file in "$P2_RUN_DIR"/sql/hive_*.sql; do
    [[ -f "$file" ]] || continue
    local id
    id="$(basename "$file" .sql)"
    run_timed "hive" "$id" "$file" \
      "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$file'"
  done
}

run_trino() {
  if ! p2_check_port hadoop1 8080 "Trino coordinator" optional; then
    p2_report SKIP "trino_perf" "Trino coordinator is not running"
    return 0
  fi
  local file
  for file in "$P2_RUN_DIR"/sql/trino_*.sql; do
    [[ -f "$file" ]] || continue
    local id
    id="$(basename "$file" .sql)"
    run_timed "trino" "$id" "$file" \
      "TRINO=/export/server/trino/bin/trino; if command -v trino >/dev/null 2>&1; then TRINO=trino; fi; \$TRINO --server http://hadoop1:8080 --file '$file'"
  done
}

run_doris() {
  if ! p2_check_port hadoop1 9030 "Doris FE mysql" optional; then
    p2_report SKIP "doris_perf" "Doris FE mysql port is not running"
    return 0
  fi
  if ! command -v mysql >/dev/null 2>&1; then
    p2_report SKIP "doris_perf" "mysql CLI missing on current host"
    return 0
  fi
  local file
  for file in "$P2_RUN_DIR"/sql/doris_*.sql; do
    [[ -f "$file" ]] || continue
    local id
    id="$(basename "$file" .sql)"
    run_timed "doris" "$id" "$file" \
      "mysql -h 192.168.88.101 -P 9030 -uroot < '$file'"
  done
}

write_sql_files

IFS=',' read -r -a ENGINE_LIST <<< "$ENGINES"
for engine in "${ENGINE_LIST[@]}"; do
  case "$engine" in
    hive) run_hive ;;
    trino) run_trino ;;
    doris) run_doris ;;
    "")
      ;;
    *)
      p2_report FAIL "engine_argument" "unknown engine: $engine"
      ;;
  esac
done

p2_report PASS "query_perf_results" "results=$RESULTS"
p2_finish

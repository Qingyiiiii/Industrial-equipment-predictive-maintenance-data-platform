#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
START_TRINO=1
START_DORIS=1
ALLOW_SWAPOFF=0
TIMEOUT_SECONDS=300
HIVE_DB="metropt_quality"
TRINO_SCHEMA="iceberg.metropt_quality_iceberg"
DORIS_DB="metropt_quality_olap"
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p12_query_layer_validation.sh [options]

Options:
  --skip-start              Do not start Trino/Doris; run checks against current services.
  --skip-trino-start        Do not start Trino.
  --skip-doris-start        Do not start Doris.
  --allow-swapoff           Allow delegated Doris startup to run sudo swapoff -a.
  --timeout SECONDS         Per-query timeout, default: 300.
  --hosts "hadoop1 hadoop2 hadoop3"
  --hive-db NAME            Default: metropt_quality.
  --trino-schema NAME       Default: iceberg.metropt_quality_iceberg.
  --doris-db NAME           Default: metropt_quality_olap.
  -h, --help                Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-start)
      START_TRINO=0
      START_DORIS=0
      shift
      ;;
    --skip-trino-start)
      START_TRINO=0
      shift
      ;;
    --skip-doris-start)
      START_DORIS=0
      shift
      ;;
    --allow-swapoff)
      ALLOW_SWAPOFF=1
      shift
      ;;
    --timeout)
      TIMEOUT_SECONDS="${2:-}"
      shift 2
      ;;
    --hosts)
      read -r -a HOSTS <<< "${2:-}"
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

if [[ "$TIMEOUT_SECONDS" =~ [^0-9] || -z "$TIMEOUT_SECONDS" ]]; then
  echo "--timeout must be a non-negative integer" >&2
  exit 2
fi

p2_init "p12_query_layer_validation" "${ORIGINAL_ARGS[@]}"
p2_header "P12 MetroPT P9 query-layer validation"
printf 'start_trino=%s\nstart_doris=%s\nallow_swapoff=%s\ntimeout=%s\nhosts=%s\nhive_db=%s\ntrino_schema=%s\ndoris_db=%s\n\n' \
  "$START_TRINO" "$START_DORIS" "$ALLOW_SWAPOFF" "$TIMEOUT_SECONDS" "${HOSTS[*]}" "$HIVE_DB" "$TRINO_SCHEMA" "$DORIS_DB"

MYSQL="mysql -h 127.0.0.1 -P 9030 -uroot --batch --raw"
RESULTS="$P2_RUN_DIR/p12_query_results.tsv"
CONSISTENCY="$P2_RUN_DIR/p12_consistency.tsv"
SQL_DIR="$P2_RUN_DIR/sql"
EXPORT_DIR="$P2_RUN_DIR/exports"
mkdir -p "$SQL_DIR" "$EXPORT_DIR"
printf 'engine\tquery_id\treturn_code\treal_seconds\trows\tlog\toutput\n' > "$RESULTS"
printf 'check\tstatus\texpected\tactual\tnote\n' > "$CONSISTENCY"

record_consistency() {
  local check="$1"
  local status="$2"
  local expected="$3"
  local actual="$4"
  local note="${5:-}"
  printf '%s\t%s\t%s\t%s\t%s\n' "$check" "$status" "$expected" "$actual" "$note" >> "$CONSISTENCY"
  if [[ "$status" == "PASS" ]]; then
    p2_report PASS "$check" "expected=$expected actual=$actual ${note:-}"
  else
    p2_report FAIL "$check" "expected=$expected actual=$actual ${note:-}"
  fi
}

write_p9_sql_files() {
  cat > "$SQL_DIR/hive_01_p9_window_dashboard.sql" <<SQL
USE $HIVE_DB;
SELECT
  dt,
  operating_state,
  COUNT(*) AS minute_count,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count,
  AVG(failure_window_rate) AS avg_failure_window_rate,
  AVG(avg_oil_temperature) AS avg_oil_temperature,
  AVG(avg_motor_current) AS avg_motor_current
FROM vw_pbi_metropt_window_kpi
GROUP BY dt, operating_state
ORDER BY dt, operating_state
LIMIT 100;
SQL
  cat > "$SQL_DIR/hive_02_p9_sensor_dashboard.sql" <<SQL
USE $HIVE_DB;
SELECT
  sensor_name,
  sensor_type,
  unit,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM vw_pbi_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;
SQL
  cat > "$SQL_DIR/trino_01_p9_ods_count.sql" <<SQL
SELECT COUNT(*) AS ods_rows
FROM $TRINO_SCHEMA.ods_metropt_readings;
SQL
  cat > "$SQL_DIR/trino_02_p9_sensor_long_counts.sql" <<SQL
SELECT
  sensor_name,
  sensor_type,
  COUNT(*) AS rows_in_long_table
FROM $TRINO_SCHEMA.dwd_metropt_sensor_long
GROUP BY sensor_name, sensor_type
ORDER BY sensor_name;
SQL
  cat > "$SQL_DIR/trino_03_p9_window_consistency.sql" <<SQL
SELECT
  COUNT(*) AS window_rows,
  SUM(sample_count) AS sample_count,
  SUM(failure_sample_count) AS failure_sample_count
FROM $TRINO_SCHEMA.dws_metropt_window_kpi;
SQL
  cat > "$SQL_DIR/doris_01_p9_sensor_dashboard.sql" <<SQL
USE $DORIS_DB;
SELECT
  sensor_name,
  sensor_type,
  sample_count,
  failure_sample_count,
  failure_window_rate,
  avg_sensor_value,
  std_sensor_value
FROM dws_metropt_sensor_kpi
ORDER BY failure_window_rate DESC, sensor_name
LIMIT 15;
SQL
  cat > "$SQL_DIR/doris_02_p9_window_dashboard.sql" <<SQL
USE $DORIS_DB;
SELECT
  dt,
  operating_state,
  minute_count,
  sample_count,
  failure_sample_count,
  avg_failure_window_rate,
  avg_oil_temperature,
  avg_motor_current
FROM p12_metropt_window_state_kpi
ORDER BY dt, operating_state
LIMIT 100;
SQL
  cat > "$SQL_DIR/doris_03_p9_consistency.sql" <<SQL
USE $DORIS_DB;
SELECT 'sensor_rows' AS metric, COUNT(*) AS value FROM dws_metropt_sensor_kpi
UNION ALL
SELECT 'sensor_sample_sum' AS metric, SUM(sample_count) AS value FROM dws_metropt_sensor_kpi
UNION ALL
SELECT 'window_state_rows' AS metric, COUNT(*) AS value FROM p12_metropt_window_state_kpi
UNION ALL
SELECT 'window_sample_sum' AS metric, SUM(sample_count) AS value FROM p12_metropt_window_state_kpi
UNION ALL
SELECT 'window_failure_sample_sum' AS metric, SUM(failure_sample_count) AS value FROM p12_metropt_window_state_kpi;
SQL
}

rows_from_output() {
  local output="$1"
  local lines
  if [[ ! -s "$output" ]]; then
    echo 0
    return
  fi
  lines="$(wc -l < "$output" | tr -d ' ')"
  if [[ "$lines" =~ ^[0-9]+$ && "$lines" -gt 0 ]]; then
    echo $((lines - 1))
  else
    echo 0
  fi
}

run_query() {
  local engine="$1"
  local query_id="$2"
  local sql_file="$3"
  local command="$4"
  local output="$P2_RUN_DIR/${engine}_${query_id}.tsv"
  local log="$P2_RUN_DIR/${engine}_${query_id}.log"
  local start end rc seconds rows
  start="$(date +%s)"
  {
    printf 'engine=%s\nquery_id=%s\nsql_file=%s\nstart=%s\ncommand=%s\n\n' "$engine" "$query_id" "$sql_file" "$(date '+%F %T')" "$command"
  } > "$log"
  timeout "$TIMEOUT_SECONDS" bash -lc "$command" > "$output" 2>> "$log"
  rc=$?
  end="$(date +%s)"
  seconds=$((end - start))
  rows="$(rows_from_output "$output")"
  {
    printf '\n[output]\n'
    cat "$output" 2>/dev/null || true
    printf '\nend=%s\nreturn_code=%s\nreal_seconds=%s\nrows=%s\n' "$(date '+%F %T')" "$rc" "$seconds" "$rows"
  } >> "$log"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$engine" "$query_id" "$rc" "$seconds" "$rows" "$log" "$output" >> "$RESULTS"
  if ((rc == 0)); then
    p2_report PASS "${engine}_${query_id}" "rc=0 seconds=$seconds rows=$rows log=$log"
  else
    p2_report FAIL "${engine}_${query_id}" "rc=$rc seconds=$seconds rows=$rows log=$log"
    p2_failure_template "${engine}_${query_id}" "$engine" "$rc" "$log" "$(p2_extract_application_id "$log")" \
      "$engine P9 query failed or timed out." \
      "tail -n 120 '$log'"
  fi
}

start_query_engines() {
  if ((START_TRINO == 1)); then
    p2_run_logged "start_trino_extended_mode" "Trino" "$P2_RUN_DIR/start_trino_extended_mode.log" \
      "Trino extended query mode failed to start." \
      "cd $P2_PROJECT_ROOT && bin/start_extended_query_mode.sh --trino-only" \
      bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_extended_query_mode.sh --trino-only"
  else
    p2_report SKIP "start_trino_extended_mode" "skipped by option"
  fi

  if ((START_DORIS == 1)); then
    local args="--doris-only"
    if ((ALLOW_SWAPOFF == 1)); then
      args="$args --allow-swapoff"
    fi
    p2_run_logged "start_doris_extended_mode" "Doris" "$P2_RUN_DIR/start_doris_extended_mode.log" \
      "Doris extended query mode failed to start." \
      "cd $P2_PROJECT_ROOT && bin/start_extended_query_mode.sh --doris-only --allow-swapoff" \
      bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_extended_query_mode.sh $args"
  else
    p2_report SKIP "start_doris_extended_mode" "skipped by option"
  fi
}

validate_engine_ports() {
  p2_check_port hadoop1 10000 "HiveServer2" required || true
  p2_check_port hadoop1 8080 "Trino coordinator" required || true
  p2_check_port hadoop1 9030 "Doris FE mysql" required || true
}

run_hive_p9_samples() {
  local file id
  for file in "$SQL_DIR"/hive_*.sql; do
    [[ -f "$file" ]] || continue
    id="$(basename "$file" .sql)"
    run_query "hive" "$id" "$file" \
      "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=true --outputformat=tsv2 -f '$file'"
  done
}

run_trino_p9_samples() {
  local file id
  for file in "$SQL_DIR"/trino_*.sql; do
    [[ -f "$file" ]] || continue
    id="$(basename "$file" .sql)"
    run_query "trino" "$id" "$file" \
      "TRINO=/export/server/trino/bin/trino; if command -v trino >/dev/null 2>&1; then TRINO=trino; fi; \$TRINO --server http://hadoop1:8080 --output-format TSV_HEADER --file '$file'"
  done
}

scalar_hive() {
  local sql="$1"
  local file="$P2_RUN_DIR/hive_scalar.sql"
  printf 'USE %s;\n%s\n' "$HIVE_DB" "$sql" > "$file"
  bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=false --outputformat=tsv2 -f '$file'" 2>/dev/null \
    | awk 'NF {last=$0} END {gsub(/\r/, "", last); print last}'
}

scalar_trino() {
  local sql="$1"
  local file="$P2_RUN_DIR/trino_scalar.sql"
  printf '%s\n' "$sql" > "$file"
  bash -lc "TRINO=/export/server/trino/bin/trino; if command -v trino >/dev/null 2>&1; then TRINO=trino; fi; \$TRINO --server http://hadoop1:8080 --output-format TSV --file '$file'" 2>/dev/null \
    | awk 'NF {last=$0} END {gsub(/\r/, "", last); print last}'
}

validate_trino_consistency() {
  local hive_ods_rows trino_ods_rows
  local hive_dwd_rows trino_dwd_rows hive_dwd_groups trino_dwd_groups
  local hive_window_rows trino_window_rows hive_window_sum trino_window_sum
  local hive_window_failure_sum trino_window_failure_sum

  hive_ods_rows="$(scalar_hive "SELECT COUNT(*) FROM ods_metropt_readings;")"
  trino_ods_rows="$(scalar_trino "SELECT COUNT(*) FROM $TRINO_SCHEMA.ods_metropt_readings;")"
  hive_dwd_rows="$(scalar_hive "SELECT COUNT(*) FROM dwd_metropt_sensor_long;")"
  trino_dwd_rows="$(scalar_trino "SELECT COUNT(*) FROM $TRINO_SCHEMA.dwd_metropt_sensor_long;")"
  hive_dwd_groups="$(scalar_hive "SELECT COUNT(*) FROM (SELECT sensor_name, sensor_type, COUNT(*) AS rows_in_long_table FROM dwd_metropt_sensor_long GROUP BY sensor_name, sensor_type) t;")"
  trino_dwd_groups="$(scalar_trino "SELECT COUNT(*) FROM (SELECT sensor_name, sensor_type, COUNT(*) AS rows_in_long_table FROM $TRINO_SCHEMA.dwd_metropt_sensor_long GROUP BY sensor_name, sensor_type) t;")"
  hive_window_rows="$(scalar_hive "SELECT COUNT(*) FROM dws_metropt_window_kpi;")"
  trino_window_rows="$(scalar_trino "SELECT COUNT(*) FROM $TRINO_SCHEMA.dws_metropt_window_kpi;")"
  hive_window_sum="$(scalar_hive "SELECT CAST(SUM(sample_count) AS BIGINT) FROM dws_metropt_window_kpi;")"
  trino_window_sum="$(scalar_trino "SELECT CAST(SUM(sample_count) AS BIGINT) FROM $TRINO_SCHEMA.dws_metropt_window_kpi;")"
  hive_window_failure_sum="$(scalar_hive "SELECT CAST(SUM(failure_sample_count) AS BIGINT) FROM dws_metropt_window_kpi;")"
  trino_window_failure_sum="$(scalar_trino "SELECT CAST(SUM(failure_sample_count) AS BIGINT) FROM $TRINO_SCHEMA.dws_metropt_window_kpi;")"

  [[ "$trino_ods_rows" == "$hive_ods_rows" ]] \
    && record_consistency "ods_rows_hive_vs_trino" "PASS" "$hive_ods_rows" "$trino_ods_rows" "ods_metropt_readings" \
    || record_consistency "ods_rows_hive_vs_trino" "FAIL" "$hive_ods_rows" "$trino_ods_rows" "ods_metropt_readings"
  [[ "$trino_dwd_rows" == "$hive_dwd_rows" ]] \
    && record_consistency "dwd_sensor_long_rows_hive_vs_trino" "PASS" "$hive_dwd_rows" "$trino_dwd_rows" "dwd_metropt_sensor_long" \
    || record_consistency "dwd_sensor_long_rows_hive_vs_trino" "FAIL" "$hive_dwd_rows" "$trino_dwd_rows" "dwd_metropt_sensor_long"
  [[ "$trino_dwd_groups" == "$hive_dwd_groups" ]] \
    && record_consistency "dwd_sensor_long_groups_hive_vs_trino" "PASS" "$hive_dwd_groups" "$trino_dwd_groups" "P9 sensor long grouped query shape" \
    || record_consistency "dwd_sensor_long_groups_hive_vs_trino" "FAIL" "$hive_dwd_groups" "$trino_dwd_groups" "P9 sensor long grouped query shape"
  [[ "$trino_window_rows" == "$hive_window_rows" ]] \
    && record_consistency "dws_window_rows_hive_vs_trino" "PASS" "$hive_window_rows" "$trino_window_rows" "dws_metropt_window_kpi" \
    || record_consistency "dws_window_rows_hive_vs_trino" "FAIL" "$hive_window_rows" "$trino_window_rows" "dws_metropt_window_kpi"
  [[ "$trino_window_sum" == "$hive_window_sum" ]] \
    && record_consistency "dws_window_sample_sum_hive_vs_trino" "PASS" "$hive_window_sum" "$trino_window_sum" "dws_metropt_window_kpi" \
    || record_consistency "dws_window_sample_sum_hive_vs_trino" "FAIL" "$hive_window_sum" "$trino_window_sum" "dws_metropt_window_kpi"
  [[ "$trino_window_failure_sum" == "$hive_window_failure_sum" ]] \
    && record_consistency "dws_window_failure_sum_hive_vs_trino" "PASS" "$hive_window_failure_sum" "$trino_window_failure_sum" "dws_metropt_window_kpi" \
    || record_consistency "dws_window_failure_sum_hive_vs_trino" "FAIL" "$hive_window_failure_sum" "$trino_window_failure_sum" "dws_metropt_window_kpi"
}

write_doris_ddl() {
  cat > "$SQL_DIR/doris_p12_ddl.sql" <<SQL
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
PROPERTIES ("replication_num" = "1");
TRUNCATE TABLE dws_metropt_sensor_kpi;
CREATE TABLE IF NOT EXISTS p12_metropt_window_state_kpi (
  dt VARCHAR(32) NOT NULL,
  operating_state VARCHAR(32) NOT NULL,
  minute_count BIGINT,
  sample_count BIGINT,
  failure_sample_count BIGINT,
  avg_failure_window_rate DOUBLE,
  avg_oil_temperature DOUBLE,
  avg_motor_current DOUBLE
)
DUPLICATE KEY(dt, operating_state)
DISTRIBUTED BY HASH(dt) BUCKETS 3
PROPERTIES ("replication_num" = "1");
TRUNCATE TABLE p12_metropt_window_state_kpi;
SQL
}

export_hive_for_doris() {
  local sensor_out="$EXPORT_DIR/dws_metropt_sensor_kpi.tsv"
  local window_out="$EXPORT_DIR/p12_metropt_window_state_kpi.tsv"
  p2_run_logged "export_hive_sensor_kpi_for_doris" "Hive" "$P2_RUN_DIR/export_hive_sensor_kpi_for_doris.log" \
    "Hive sensor KPI export for P12 Doris load failed." \
    "tail -n 120 '$P2_RUN_DIR/export_hive_sensor_kpi_for_doris.log'" \
    bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=false --outputformat=tsv2 -e \"USE $HIVE_DB; SELECT sensor_name, sensor_type, station_id, unit, sample_count, failure_sample_count, avg_sensor_value, std_sensor_value, min_sensor_value, max_sensor_value, failure_window_rate FROM dws_metropt_sensor_kpi ORDER BY sensor_name\" > '$sensor_out'"
  p2_run_logged "export_hive_window_state_for_doris" "Hive" "$P2_RUN_DIR/export_hive_window_state_for_doris.log" \
    "Hive window dashboard export for P12 Doris load failed." \
    "tail -n 120 '$P2_RUN_DIR/export_hive_window_state_for_doris.log'" \
    bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --silent=true --showHeader=false --outputformat=tsv2 -e \"USE $HIVE_DB; SELECT dt, operating_state, COUNT(*) AS minute_count, SUM(sample_count) AS sample_count, SUM(failure_sample_count) AS failure_sample_count, AVG(failure_window_rate) AS avg_failure_window_rate, AVG(avg_oil_temperature) AS avg_oil_temperature, AVG(avg_motor_current) AS avg_motor_current FROM vw_pbi_metropt_window_kpi GROUP BY dt, operating_state ORDER BY dt, operating_state\" > '$window_out'"
}

load_doris_p9_tables() {
  write_doris_ddl
  export_hive_for_doris
  p2_run_logged "doris_create_p12_tables" "Doris" "$P2_RUN_DIR/doris_create_p12_tables.log" \
    "Doris P12 DDL failed." \
    "tail -n 120 '$P2_RUN_DIR/doris_create_p12_tables.log'" \
    bash -lc "$MYSQL < '$SQL_DIR/doris_p12_ddl.sql'"

  cat > "$SQL_DIR/doris_load_sensor_kpi.sql" <<SQL
USE $DORIS_DB;
LOAD DATA LOCAL INFILE '$EXPORT_DIR/dws_metropt_sensor_kpi.tsv'
INTO TABLE dws_metropt_sensor_kpi
COLUMNS TERMINATED BY '\t'
(sensor_name, sensor_type, station_id, unit, sample_count, failure_sample_count, avg_sensor_value, std_sensor_value, min_sensor_value, max_sensor_value, failure_window_rate);
SQL
  cat > "$SQL_DIR/doris_load_window_state.sql" <<SQL
USE $DORIS_DB;
LOAD DATA LOCAL INFILE '$EXPORT_DIR/p12_metropt_window_state_kpi.tsv'
INTO TABLE p12_metropt_window_state_kpi
COLUMNS TERMINATED BY '\t'
(dt, operating_state, minute_count, sample_count, failure_sample_count, avg_failure_window_rate, avg_oil_temperature, avg_motor_current);
SQL
  p2_run_logged "doris_load_sensor_kpi" "Doris" "$P2_RUN_DIR/doris_load_sensor_kpi.log" \
    "Doris sensor KPI LOAD DATA failed." \
    "tail -n 120 '$P2_RUN_DIR/doris_load_sensor_kpi.log'" \
    bash -lc "$MYSQL --local-infile=1 < '$SQL_DIR/doris_load_sensor_kpi.sql'"
  p2_run_logged "doris_load_window_state_kpi" "Doris" "$P2_RUN_DIR/doris_load_window_state_kpi.log" \
    "Doris window-state KPI LOAD DATA failed." \
    "tail -n 120 '$P2_RUN_DIR/doris_load_window_state_kpi.log'" \
    bash -lc "$MYSQL --local-infile=1 < '$SQL_DIR/doris_load_window_state.sql'"
}

run_doris_p9_samples() {
  local file id
  for file in "$SQL_DIR"/doris_0*.sql; do
    [[ -f "$file" ]] || continue
    id="$(basename "$file" .sql)"
    run_query "doris" "$id" "$file" \
      "$MYSQL < '$file'"
  done
}

scalar_mysql() {
  local sql="$1"
  bash -lc "$MYSQL --skip-column-names -e $(printf '%q' "$sql")" 2>/dev/null | tail -n 1 | tr -d '\r'
}

validate_consistency() {
  local sensor_export="$EXPORT_DIR/dws_metropt_sensor_kpi.tsv"
  local window_export="$EXPORT_DIR/p12_metropt_window_state_kpi.tsv"
  local hive_sensor_rows hive_window_rows hive_sensor_sum hive_window_sum hive_window_failure_sum
  local doris_sensor_rows doris_window_rows doris_sensor_sum doris_window_sum doris_window_failure_sum

  hive_sensor_rows="$(wc -l < "$sensor_export" | tr -d ' ')"
  hive_window_rows="$(wc -l < "$window_export" | tr -d ' ')"
  hive_sensor_sum="$(awk -F'\t' '{s+=$5} END{printf "%.0f", s}' "$sensor_export")"
  hive_window_sum="$(awk -F'\t' '{s+=$4} END{printf "%.0f", s}' "$window_export")"
  hive_window_failure_sum="$(awk -F'\t' '{s+=$5} END{printf "%.0f", s}' "$window_export")"
  doris_sensor_rows="$(scalar_mysql "USE $DORIS_DB; SELECT COUNT(*) FROM dws_metropt_sensor_kpi;")"
  doris_window_rows="$(scalar_mysql "USE $DORIS_DB; SELECT COUNT(*) FROM p12_metropt_window_state_kpi;")"
  doris_sensor_sum="$(scalar_mysql "USE $DORIS_DB; SELECT CAST(SUM(sample_count) AS BIGINT) FROM dws_metropt_sensor_kpi;")"
  doris_window_sum="$(scalar_mysql "USE $DORIS_DB; SELECT CAST(SUM(sample_count) AS BIGINT) FROM p12_metropt_window_state_kpi;")"
  doris_window_failure_sum="$(scalar_mysql "USE $DORIS_DB; SELECT CAST(SUM(failure_sample_count) AS BIGINT) FROM p12_metropt_window_state_kpi;")"

  [[ "$doris_sensor_rows" == "$hive_sensor_rows" ]] \
    && record_consistency "sensor_rows_hive_vs_doris" "PASS" "$hive_sensor_rows" "$doris_sensor_rows" "dws_metropt_sensor_kpi" \
    || record_consistency "sensor_rows_hive_vs_doris" "FAIL" "$hive_sensor_rows" "$doris_sensor_rows" "dws_metropt_sensor_kpi"
  [[ "$doris_sensor_sum" == "$hive_sensor_sum" ]] \
    && record_consistency "sensor_sample_sum_hive_vs_doris" "PASS" "$hive_sensor_sum" "$doris_sensor_sum" "dws_metropt_sensor_kpi" \
    || record_consistency "sensor_sample_sum_hive_vs_doris" "FAIL" "$hive_sensor_sum" "$doris_sensor_sum" "dws_metropt_sensor_kpi"
  [[ "$doris_window_rows" == "$hive_window_rows" ]] \
    && record_consistency "window_state_rows_hive_vs_doris" "PASS" "$hive_window_rows" "$doris_window_rows" "p12_metropt_window_state_kpi" \
    || record_consistency "window_state_rows_hive_vs_doris" "FAIL" "$hive_window_rows" "$doris_window_rows" "p12_metropt_window_state_kpi"
  [[ "$doris_window_sum" == "$hive_window_sum" ]] \
    && record_consistency "window_sample_sum_hive_vs_doris" "PASS" "$hive_window_sum" "$doris_window_sum" "p12_metropt_window_state_kpi" \
    || record_consistency "window_sample_sum_hive_vs_doris" "FAIL" "$hive_window_sum" "$doris_window_sum" "p12_metropt_window_state_kpi"
  [[ "$doris_window_failure_sum" == "$hive_window_failure_sum" ]] \
    && record_consistency "window_failure_sum_hive_vs_doris" "PASS" "$hive_window_failure_sum" "$doris_window_failure_sum" "p12_metropt_window_state_kpi" \
    || record_consistency "window_failure_sum_hive_vs_doris" "FAIL" "$hive_window_failure_sum" "$doris_window_failure_sum" "p12_metropt_window_state_kpi"
}

write_summary_doc() {
  local doc="$P2_RUN_DIR/p12_query_layer_summary.md"
  cat > "$doc" <<EOF
# P12 Query Layer Validation Summary

- run_id: $P2_RUN_ID
- hive_db: $HIVE_DB
- trino_schema: $TRINO_SCHEMA
- doris_db: $DORIS_DB
- results: \`$RESULTS\`
- consistency: \`$CONSISTENCY\`

## SQL Sources

The Trino and Doris queries in this run are generated from the P9 dashboard field dictionary scope. This is a fresh P12 validation run and does not reuse old P5 smoke results as P9 evidence.

## Results

\`\`\`tsv
$(cat "$RESULTS")
\`\`\`

## Consistency

\`\`\`tsv
$(cat "$CONSISTENCY")
\`\`\`
EOF
  p2_report PASS "p12_query_layer_summary" "summary=$doc"
}

write_p9_sql_files
start_query_engines
validate_engine_ports
run_hive_p9_samples
run_trino_p9_samples
validate_trino_consistency
load_doris_p9_tables
run_doris_p9_samples
validate_consistency
write_summary_doc

p2_finish

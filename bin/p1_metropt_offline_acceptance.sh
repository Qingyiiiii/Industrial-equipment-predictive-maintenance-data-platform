#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p1_common.sh"

CONFIG="/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml"
PYTHON_EXEC="${PYTHON_EXEC:-python3}"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
RUNNER_EXECUTOR="python"
SKIP_RUN=0
SKIP_TRINO=0
REQUIRE_TRINO=0
START_AT=""
STOP_AFTER=""

RAW_CSV="hdfs:///lakehouse/projects/metropt_quality/raw/MetroPT3_AirCompressor.csv"
ODS_PARQUET="hdfs:///lakehouse/projects/metropt_quality/ods/readings"
DWD_PARQUET="hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long"
DWS_WINDOW_PARQUET="hdfs:///lakehouse/projects/metropt_quality/dws/window_kpi"
DWS_SENSOR_PARQUET="hdfs:///lakehouse/projects/metropt_quality/dws/sensor_kpi"

usage() {
  cat <<'USAGE'
Usage:
  bin/p1_metropt_offline_acceptance.sh [options]

Options:
  --config PATH              MetroPT cluster config.
  --skip-run                 Skip 00-06 offline runner and only verify existing outputs.
  --runner-executor MODE     python or spark-submit, default: python.
  --start-at STEP            Forwarded to src/run_metropt_offline.py.
  --stop-after STEP          Forwarded to src/run_metropt_offline.py.
  --skip-trino               Skip Trino/Iceberg checks and exit 0 if required checks pass.
  --require-trino            Treat Trino not running as FAIL instead of exit 4.
  -h, --help                 Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --skip-run)
      SKIP_RUN=1
      shift
      ;;
    --runner-executor)
      RUNNER_EXECUTOR="${2:-}"
      shift 2
      ;;
    --start-at)
      START_AT="${2:-}"
      shift 2
      ;;
    --stop-after)
      STOP_AFTER="${2:-}"
      shift 2
      ;;
    --skip-trino)
      SKIP_TRINO=1
      shift
      ;;
    --require-trino)
      REQUIRE_TRINO=1
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

if [[ "$RUNNER_EXECUTOR" != "python" && "$RUNNER_EXECUTOR" != "spark-submit" ]]; then
  echo "--runner-executor must be python or spark-submit" >&2
  exit 2
fi

p1_init "offline_acceptance"
p1_header "P1 MetroPT offline acceptance"

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

run_required_gate "p0_config_drift" "bin/p0_config_drift_check.sh"
run_required_gate "p0_hdfs_yarn" "bin/p0_cluster_health_check.sh --module hdfs-yarn"
run_required_gate "p0_hive" "bin/p0_cluster_health_check.sh --module hive"

if ((SKIP_RUN == 1)); then
  p1_report WARN "offline_runner" "skipped by --skip-run"
else
  runner_args=()
  [[ -n "$START_AT" ]] && runner_args+=(--start-at "$START_AT")
  [[ -n "$STOP_AFTER" ]] && runner_args+=(--stop-after "$STOP_AFTER")
  runner_args+=(--log-dir "$P1_LOG_DIR/offline_runner")

  if [[ "$RUNNER_EXECUTOR" == "spark-submit" ]]; then
    runner_cmd=(bash -lc "cd '$P1_PROJECT_ROOT' && export METROPT_CONFIG='$CONFIG' JAVA_HOME=/export/server/jdk17 && export PATH=/usr/local/bin:/usr/bin:/bin:\$JAVA_HOME/bin:\$PATH && '$SPARK_SUBMIT' src/run_metropt_offline.py ${runner_args[*]@Q}")
  else
    runner_cmd=(bash -lc "cd '$P1_PROJECT_ROOT' && export METROPT_CONFIG='$CONFIG' JAVA_HOME=/export/server/jdk17 && export PATH=/usr/local/bin:/usr/bin:/bin:\$JAVA_HOME/bin:\$PATH && '$PYTHON_EXEC' src/run_metropt_offline.py --spark-submit '$SPARK_SUBMIT' ${runner_args[*]@Q}")
  fi

  p1_run_logged "offline_runner_00_06" "Spark/YARN" "$P1_LOG_DIR/offline_runner_00_06.log" \
    "Spark offline runner failed. Check the step log inside $P1_LOG_DIR/offline_runner and YARN application logs." \
    "grep -R \"application_\" '$P1_LOG_DIR/offline_runner'; yarn logs -applicationId <application_id> -appOwner common" \
    "${runner_cmd[@]}"
fi

p1_run_logged "hdfs_output_paths" "HDFS" "$P1_LOG_DIR/hdfs_output_paths.log" \
  "Required raw or Parquet path is missing in HDFS." \
  "hdfs dfs -ls /lakehouse/projects/metropt_quality" \
  bash -lc "export JAVA_HOME=/export/server/jdk17; /export/server/hadoop/bin/hdfs dfs -test -f '$RAW_CSV' && /export/server/hadoop/bin/hdfs dfs -test -d '$ODS_PARQUET' && /export/server/hadoop/bin/hdfs dfs -test -d '$DWD_PARQUET' && /export/server/hadoop/bin/hdfs dfs -test -d '$DWS_WINDOW_PARQUET' && /export/server/hadoop/bin/hdfs dfs -test -d '$DWS_SENSOR_PARQUET'"

probe_py="$P1_LOG_DIR/p1_offline_parquet_probe.py"
cat > "$probe_py" <<PY
from pyspark.sql import SparkSession

paths = [
    ("ods_readings", "$ODS_PARQUET"),
    ("dwd_sensor_long", "$DWD_PARQUET"),
    ("dws_window_kpi", "$DWS_WINDOW_PARQUET"),
    ("dws_sensor_kpi", "$DWS_SENSOR_PARQUET"),
]

spark = SparkSession.builder.appName("P1_MetroPT_Offline_Parquet_Probe").getOrCreate()
for label, path in paths:
    df = spark.read.parquet(path)
    print(f"{label}\\tcount\\t{df.count()}")
    df.limit(5).show(5, truncate=False)
spark.stop()
PY

p1_run_logged "spark_parquet_probe" "Spark/YARN" "$P1_LOG_DIR/spark_parquet_probe.log" \
  "Spark failed while reading MetroPT Parquet outputs." \
  "yarn logs -applicationId <application_id> -appOwner common" \
  bash -lc "export JAVA_HOME=/export/server/jdk17; '$SPARK_SUBMIT' '$probe_py'"

hive_sql="$P1_LOG_DIR/offline_hive_counts.sql"
cat > "$hive_sql" <<'SQL'
USE metropt_quality;
SHOW TABLES;
SELECT 'ods_metropt_readings', COUNT(*) FROM ods_metropt_readings;
SELECT 'dwd_metropt_sensor_long', COUNT(*) FROM dwd_metropt_sensor_long;
SELECT 'dws_metropt_window_kpi', COUNT(*) FROM dws_metropt_window_kpi;
SELECT 'dws_metropt_sensor_kpi', COUNT(*) FROM dws_metropt_sensor_kpi;
SQL

hive_log="$P1_LOG_DIR/offline_hive_counts.log"
p1_run_logged "offline_hive_counts_plain" "Hive" "$hive_log" \
  "Plain Beeline Hive COUNT failed." \
  "cd $P1_PROJECT_ROOT && bin/metropt_hive_mr_count_check.sh --mode offline" \
  bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$hive_sql'"
plain_hive_rc=$?

if ((plain_hive_rc != 0)); then
  fallback_log="$P1_LOG_DIR/offline_hive_counts_fallback.log"
  if p1_run_logged "offline_hive_counts_fallback" "Hive" "$fallback_log" \
    "Hive COUNT fallback failed; inspect Hive-on-MR/YARN logs." \
    "yarn logs -applicationId <application_id> -appOwner common" \
    bash -lc "cd '$P1_PROJECT_ROOT' && bin/metropt_hive_mr_count_check.sh --mode offline"; then
    p1_report WARN "offline_hive_counts" "plain Beeline failed but JDK8 fallback passed"
    hive_log="$fallback_log"
  fi
fi

if [[ -f "$hive_log" ]]; then
  if grep -q $'ods_metropt_readings\t1516948' "$hive_log" \
    && grep -q $'dwd_metropt_sensor_long\t22754220' "$hive_log" \
    && grep -q $'dws_metropt_window_kpi\t269991' "$hive_log" \
    && grep -q $'dws_metropt_sensor_kpi\t15' "$hive_log"; then
    p1_report PASS "offline_hive_count_baseline" "all expected counts matched"
  else
    p1_report FAIL "offline_hive_count_baseline" "expected counts not found in $hive_log"
    p1_failure_template "offline_hive_count_baseline" "Hive" 1 "$hive_log" "$(p1_extract_application_id "$hive_log")" \
      "Hive count output did not match the MetroPT offline baseline." \
      "tail -n 120 '$hive_log'"
  fi
fi

if ((SKIP_TRINO == 1)); then
  p1_report WARN "trino_iceberg" "skipped by --skip-trino"
elif ! ss -lntp 2>/dev/null | grep -qE '[:.]8080[[:space:]]'; then
  if ((REQUIRE_TRINO == 1)); then
    p1_report FAIL "trino_iceberg" "Trino coordinator port 8080 is not listening"
    p1_failure_template "trino_iceberg" "Trino" 1 "$P1_LOG_DIR/trino_iceberg.log" "" \
      "Trino is not running; Iceberg/Trino acceptance cannot execute." \
      "for h in hadoop1 hadoop2 hadoop3; do ssh common@\$h 'export JAVA_HOME=/export/server/jdk25; /export/server/trino/bin/launcher start'; done"
  else
    P1_EXTENSION_SKIPPED=1
    p1_report WARN "trino_iceberg" "Trino not running; offline Spark/Hive acceptance can still pass"
    p1_failure_template "trino_iceberg" "Trino" 4 "$P1_LOG_DIR/trino_iceberg.log" "" \
      "Trino coordinator port 8080 is not listening." \
      "bin/p0_cluster_health_check.sh --module trino-doris"
  fi
else
  trino_cmd="if command -v trino >/dev/null 2>&1; then TRINO=trino; else TRINO=/export/server/trino/bin/trino; fi; \$TRINO --server http://hadoop1:8080 --execute \"SHOW SCHEMAS FROM iceberg\"; \$TRINO --server http://hadoop1:8080 --execute \"SHOW TABLES FROM iceberg.metropt_quality_iceberg\"; \$TRINO --server http://hadoop1:8080 --execute \"SELECT COUNT(*) FROM iceberg.metropt_quality_iceberg.ods_metropt_readings\""
  p1_run_logged "trino_iceberg" "Trino" "$P1_LOG_DIR/trino_iceberg.log" \
    "Trino/Iceberg query failed." \
    "/export/server/trino/bin/launcher status; tail -n 120 /export/data/trino/var/log/server.log" \
    bash -lc "$trino_cmd"
fi

p1_finish

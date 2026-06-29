#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

CONFIG="/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml"
SPARK_SUBMIT="${SPARK_SUBMIT:-spark-submit}"
SKIP_SPARK=0
SKIP_HIVE=0
SKIP_REALTIME=0
SPARK_TIMEOUT=600

ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p3_data_quality_check.sh [options]

Options:
  --config PATH            MetroPT cluster config.
  --skip-spark             Skip Spark Parquet data quality probe.
  --skip-hive              Skip Hive table count check.
  --skip-realtime          Skip realtime Hive/Redis snapshot.
  --spark-timeout SECONDS  Timeout for Spark probe, default: 600.
  -h, --help               Show help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --skip-spark)
      SKIP_SPARK=1
      shift
      ;;
    --skip-hive)
      SKIP_HIVE=1
      shift
      ;;
    --skip-realtime)
      SKIP_REALTIME=1
      shift
      ;;
    --spark-timeout)
      SPARK_TIMEOUT="${2:-}"
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

if [[ "$SPARK_TIMEOUT" =~ [^0-9] || -z "$SPARK_TIMEOUT" ]]; then
  echo "--spark-timeout must be a non-negative integer" >&2
  exit 2
fi

p2_init "p3_data_quality" "${ORIGINAL_ARGS[@]}"
p2_header "P3 MetroPT data quality check"
printf 'config=%s\nskip_spark=%s\nskip_hive=%s\nskip_realtime=%s\n\n' "$CONFIG" "$SKIP_SPARK" "$SKIP_HIVE" "$SKIP_REALTIME"

METRICS="$P2_RUN_DIR/data_quality_metrics.tsv"
printf 'level\tmetric\tactual\texpected\tdetail\n' > "$METRICS"

write_spark_probe() {
  local probe="$P2_RUN_DIR/p3_data_quality_probe.py"
  cat > "$probe" <<'PY'
import argparse
import os
import sys

from pyspark.sql import functions as F

parser = argparse.ArgumentParser()
parser.add_argument("--project-root", required=True)
parser.add_argument("--config", required=True)
parser.add_argument("--metrics", required=True)
args = parser.parse_args()

sys.path.insert(0, os.path.join(args.project_root, "src"))
from metropt_utils import ANALOG_SENSORS, DIGITAL_SENSORS, create_metropt_spark, load_metropt_config  # noqa: E402

config = load_metropt_config(args.config)
paths = config["paths"]
expected_rows = int(config.get("metropt", {}).get("expected_rows", 1516948))
expected_sensor_count = len(ANALOG_SENSORS) + len(DIGITAL_SENSORS)
expected_dwd_rows = expected_rows * expected_sensor_count

spark = create_metropt_spark("P3_MetroPT_Data_Quality", config=config)
spark.sparkContext.setLogLevel("WARN")

rows = []


def emit(level, metric, actual, expected="", detail=""):
    rows.append((level, metric, str(actual), str(expected), str(detail).replace("\n", " ")))
    print(f"{level}\t{metric}\t{actual}\t{expected}\t{detail}")


def check_equal(metric, actual, expected, detail=""):
    emit("PASS" if actual == expected else "FAIL", metric, actual, expected, detail)


def check_positive(metric, actual, detail=""):
    emit("PASS" if actual > 0 else "FAIL", metric, actual, ">0", detail)


try:
    ods = spark.read.parquet(paths["ods_readings_parquet"])
    dwd = spark.read.parquet(paths["dwd_sensor_long"])
    dws_overall = spark.read.parquet(paths["dws_overall_kpi"])
    dws_window = spark.read.parquet(paths["dws_window_kpi"])
    dws_sensor = spark.read.parquet(paths["dws_sensor_kpi"])

    ods_count = ods.count()
    dwd_count = dwd.count()
    dws_overall_count = dws_overall.count()
    dws_window_count = dws_window.count()
    dws_sensor_count = dws_sensor.count()
    distinct_sensors = dwd.select("sensor_name").distinct().count()
    dwd_sensor_types = dwd.select("sensor_type").distinct().count()

    time_row = ods.agg(
        F.min("event_time").alias("min_event_time"),
        F.max("event_time").alias("max_event_time"),
        F.countDistinct("dt").alias("active_day_count"),
        F.sum(F.col("is_failure_window").cast("long")).alias("failure_sample_count"),
    ).first()

    null_exprs = [
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in ["event_time", "operating_state"] + ANALOG_SENSORS + DIGITAL_SENSORS
    ]
    null_row = ods.agg(*null_exprs).first().asDict()
    null_total = sum(int(v or 0) for v in null_row.values())

    check_equal("ods_metropt_readings_rows", ods_count, expected_rows)
    check_equal("dwd_metropt_sensor_long_rows", dwd_count, expected_dwd_rows)
    check_equal("dwd_sensor_count", distinct_sensors, expected_sensor_count)
    check_positive("dwd_sensor_type_count", dwd_sensor_types)
    check_positive("dws_metropt_overall_kpi_rows", dws_overall_count)
    check_equal("dws_metropt_window_kpi_rows", dws_window_count, 269991)
    check_equal("dws_metropt_sensor_kpi_rows", dws_sensor_count, expected_sensor_count)
    check_equal("ods_required_field_null_total", null_total, 0)
    emit("PASS", "ods_event_time_range", f"{time_row['min_event_time']} -> {time_row['max_event_time']}", "non-empty")
    check_positive("ods_active_day_count", int(time_row["active_day_count"] or 0))
    check_positive("ods_failure_sample_count", int(time_row["failure_sample_count"] or 0))

    with open(args.metrics, "a", encoding="utf-8") as f:
        for row in rows:
            f.write("\t".join(row) + "\n")

    if any(row[0] == "FAIL" for row in rows):
        raise SystemExit(1)
finally:
    spark.stop()
PY
  printf '%s\n' "$probe"
}

run_spark_probe() {
  if ((SKIP_SPARK == 1)); then
    p2_report SKIP "spark_data_quality" "skipped by --skip-spark"
    return 0
  fi
  local probe
  probe="$(write_spark_probe)"
  p2_run_logged "spark_data_quality" "Spark/YARN" "$P2_RUN_DIR/spark_data_quality.log" \
    "Spark Parquet data-quality probe failed; inspect YARN application logs." \
    "yarn logs -applicationId <application_id> -appOwner common; tail -n 120 '$P2_RUN_DIR/spark_data_quality.log'" \
    bash -lc "cd '$P2_PROJECT_ROOT' && export METROPT_CONFIG='$CONFIG' JAVA_HOME=/export/server/jdk17 && timeout '$SPARK_TIMEOUT' '$SPARK_SUBMIT' '$probe' --project-root '$P2_PROJECT_ROOT' --config '$CONFIG' --metrics '$METRICS'"
}

run_hive_probe() {
  if ((SKIP_HIVE == 1)); then
    p2_report SKIP "hive_core_counts" "skipped by --skip-hive"
    return 0
  fi
  local hive_log="$P2_RUN_DIR/hive_core_counts.log"
  if p2_run_logged "hive_core_counts" "Hive" "$hive_log" \
    "Hive offline count baseline failed; this is a delivery blocker unless Spark Parquet metrics prove only Hive is drifting." \
    "cd $P2_PROJECT_ROOT && bin/metropt_hive_mr_count_check.sh --mode offline" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/metropt_hive_mr_count_check.sh --mode offline"; then
    if grep -q 'ods_metropt_readings' "$hive_log" && grep -q '1516948' "$hive_log"; then
      p2_report PASS "hive_core_count_baseline" "expected offline Hive counts observed"
    else
      p2_report WARN "hive_core_count_baseline" "count command rc=0 but expected baseline text was not found"
    fi
  fi
}

run_realtime_snapshot() {
  if ((SKIP_REALTIME == 1)); then
    p2_report SKIP "realtime_snapshot" "skipped by --skip-realtime"
    return 0
  fi

  local redis_log="$P2_RUN_DIR/realtime_redis_snapshot.log"
  if p2_run_on hadoop1 "redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' 2>/dev/null | head -n 20" > "$redis_log" 2>&1; then
    local key_count
    key_count="$(wc -l < "$redis_log" | tr -d ' ')"
    if [[ "$key_count" =~ ^[0-9]+$ && "$key_count" -gt 0 ]]; then
      p2_report PASS "realtime_redis_snapshot" "keys_sampled=$key_count log=$redis_log"
    else
      p2_report WARN "realtime_redis_snapshot" "no metropt:kpi:1m:* keys observed; run realtime acceptance to refresh KPI cache"
    fi
  else
    p2_report WARN "realtime_redis_snapshot" "redis scan failed; log=$redis_log"
  fi

  local hive_sql="$P2_RUN_DIR/realtime_hive_snapshot.sql"
  local hive_log="$P2_RUN_DIR/realtime_hive_snapshot.log"
  cat > "$hive_sql" <<'SQL'
USE metropt_quality;
SHOW TABLES LIKE '*realtime*';
SHOW PARTITIONS ods_metropt_realtime_readings;
SELECT * FROM ods_metropt_realtime_readings LIMIT 5;
SELECT * FROM dws_metropt_realtime_kpi_1min LIMIT 5;
SQL
  p2_run_logged "realtime_hive_snapshot" "Hive" "$hive_log" \
    "Realtime Hive snapshot failed; verify Flink sink tables and partitions." \
    "tail -n 120 '$hive_log'; cd $P2_PROJECT_ROOT && bin/p1_metropt_realtime_acceptance.sh --skip-flink-submit" \
    bash -lc "export JAVA_HOME=/export/server/jdk8; /export/server/hive/bin/beeline -u 'jdbc:hive2://hadoop1:10000/default' -n common --showHeader=false --outputformat=tsv2 -f '$hive_sql'"
}

run_spark_probe
run_hive_probe
run_realtime_snapshot

printf '\nmetrics=%s\n' "$METRICS"
p2_finish

#!/usr/bin/env bash
set -euo pipefail

MODE="offline"
DATABASE="${METROPT_HIVE_DATABASE:-metropt_quality}"
HADOOP_HOME="${HADOOP_HOME:-/export/server/hadoop}"
HIVE_HOME="${HIVE_HOME:-/export/server/hive}"
JDK8_HOME="${JDK8_HOME:-/export/server/jdk8}"
TMP_BASE="${METROPT_HIVE_MR_TMP:-/home/common/tmp}"
TMP_HADOOP_CONF="$TMP_BASE/hive-mr-jdk8-conf"
TMP_HIVE_CONF="$TMP_BASE/hive-conf-jdk8"

usage() {
  cat <<'USAGE'
Usage:
  bin/metropt_hive_mr_count_check.sh [--mode offline|realtime]

Environment overrides:
  METROPT_HIVE_DATABASE   Hive database, default: metropt_quality
  HADOOP_HOME             Hadoop home, default: /export/server/hadoop
  HIVE_HOME               Hive home, default: /export/server/hive
  JDK8_HOME               JDK 8 home, default: /export/server/jdk8
  METROPT_HIVE_MR_TMP     Temp base dir, default: /home/common/tmp
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
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

if [[ "$MODE" != "offline" && "$MODE" != "realtime" ]]; then
  echo "--mode must be offline or realtime" >&2
  exit 2
fi

for path in "$HADOOP_HOME" "$HIVE_HOME" "$JDK8_HOME"; do
  if [[ ! -d "$path" ]]; then
    echo "Required path does not exist: $path" >&2
    exit 1
  fi
done

if [[ ! -d "$HIVE_HOME/conf/hadoop-conf-jdk8" ]]; then
  echo "Missing Hive JDK8 Hadoop conf: $HIVE_HOME/conf/hadoop-conf-jdk8" >&2
  exit 1
fi

rm -rf "$TMP_HADOOP_CONF" "$TMP_HIVE_CONF"
cp -a "$HIVE_HOME/conf/hadoop-conf-jdk8" "$TMP_HADOOP_CONF"

if [[ -f "$TMP_HADOOP_CONF/hadoop-env.sh" ]]; then
  sed -i "/--add-opens=/d; /--add-exports=/d" "$TMP_HADOOP_CONF/hadoop-env.sh"
fi

cp -a "$HIVE_HOME/conf" "$TMP_HIVE_CONF"
cat > "$TMP_HIVE_CONF/hive-env.sh" <<EOF
export JAVA_HOME=$JDK8_HOME
export HADOOP_HOME=$HADOOP_HOME
export HIVE_HOME=$HIVE_HOME
export HADOOP_CONF_DIR=$TMP_HADOOP_CONF
export YARN_CONF_DIR=$TMP_HADOOP_CONF
export HIVE_CONF_DIR=$TMP_HIVE_CONF
export HADOOP_CLASSPATH="\$(JAVA_HOME=$JDK8_HOME $HADOOP_HOME/bin/hadoop --config $TMP_HADOOP_CONF classpath)"
EOF
chmod +x "$TMP_HIVE_CONF/hive-env.sh"

export JAVA_HOME="$JDK8_HOME"
export HADOOP_HOME
export HIVE_HOME
export HADOOP_CONF_DIR="$TMP_HADOOP_CONF"
export YARN_CONF_DIR="$TMP_HADOOP_CONF"
export HIVE_CONF_DIR="$TMP_HIVE_CONF"
export PATH="$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:/usr/local/bin:/usr/bin:/bin"

HIVE_MR_ENV="JAVA_HOME=$JDK8_HOME,HADOOP_MAPRED_HOME=$HADOOP_HOME"

if [[ "$MODE" == "offline" ]]; then
  SQL=$(cat <<SQL_EOF
USE $DATABASE;
SELECT COUNT(*) AS ods_metropt_readings_count FROM ods_metropt_readings;
SELECT COUNT(*) AS dwd_metropt_sensor_long_count FROM dwd_metropt_sensor_long;
SELECT COUNT(*) AS dws_metropt_window_kpi_count FROM dws_metropt_window_kpi;
SELECT COUNT(*) AS dws_metropt_sensor_kpi_count FROM dws_metropt_sensor_kpi;
SQL_EOF
)
else
  SQL=$(cat <<SQL_EOF
USE $DATABASE;
SELECT COUNT(*) AS ods_metropt_realtime_readings_count FROM ods_metropt_realtime_readings;
SELECT COUNT(*) AS dws_metropt_realtime_kpi_1min_count FROM dws_metropt_realtime_kpi_1min;
SQL_EOF
)
fi

"$HIVE_HOME/bin/hive" \
  --config "$TMP_HIVE_CONF" \
  --hiveconf mapreduce.jvm.add-opens-as-default=false \
  --hiveconf yarn.app.mapreduce.am.env="$HIVE_MR_ENV" \
  --hiveconf mapreduce.map.env="$HIVE_MR_ENV" \
  --hiveconf mapreduce.reduce.env="$HIVE_MR_ENV" \
  --hiveconf mapred.child.env="$HIVE_MR_ENV" \
  --hiveconf mapreduce.map.java.opts="-Xmx512m" \
  --hiveconf mapreduce.reduce.java.opts="-Xmx512m" \
  --hiveconf yarn.app.mapreduce.am.command-opts="-Xmx512m" \
  --hiveconf mapreduce.map.memory.mb=768 \
  --hiveconf mapreduce.reduce.memory.mb=768 \
  -S \
  -e "$SQL"

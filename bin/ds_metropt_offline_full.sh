#!/usr/bin/env bash
set -euo pipefail

cd /home/common/tmp/pycharm_Design

export METROPT_CONFIG=/home/common/tmp/pycharm_Design/config/metropt_quality.cluster.yaml
export JAVA_HOME=/export/server/jdk17
export HADOOP_HOME=/export/server/hadoop
export HADOOP_CONF_DIR=/export/server/hadoop/etc/hadoop
export SPARK_HOME=/export/server/spark
export PYSPARK_PYTHON=/usr/bin/python3
export PATH=/usr/bin:$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

echo "[MetroPT] full offline start $(date '+%F %T')"
echo "[MetroPT] host=$(hostname)"
echo "[MetroPT] pwd=$(pwd)"
echo "[MetroPT] METROPT_CONFIG=$METROPT_CONFIG"

/usr/bin/python3 src/run_metropt_offline.py \
  --spark-submit /export/server/spark/bin/spark-submit

LATEST_LOG_DIR=$(ls -td data/metropt_quality/logs/* | head -n 1)
echo "[MetroPT] latest_log_dir=$LATEST_LOG_DIR"
cat "$LATEST_LOG_DIR/offline_run_summary.tsv"

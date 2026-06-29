#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
CHECK_ONLY=0
RESTART=0
FIX_HIVE_JDK_SPLIT=0
WITH_HIVE_COUNT=0
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/start_base_services.sh [--check-only] [--restart] [--fix-hive-jdk-split] [--hive-count] [--hosts "hadoop1 hadoop2 hadoop3"]

Starts missing base services only:
  HDFS/YARN, PostgreSQL, Hive Metastore, HiveServer2, Kafka, Redis and Flink.

Defaults:
  No restart, no destructive cleanup, no Hive COUNT smoke. Use --hive-count for the full P0 Hive smoke.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only)
      CHECK_ONLY=1
      shift
      ;;
    --restart)
      RESTART=1
      shift
      ;;
    --fix-hive-jdk-split)
      FIX_HIVE_JDK_SPLIT=1
      shift
      ;;
    --hive-count)
      WITH_HIVE_COUNT=1
      shift
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

p2_init "start_base_services" "${ORIGINAL_ARGS[@]}"
p2_header "P2 base service startup"
printf 'check_only=%s\nrestart=%s\nfix_hive_jdk_split=%s\nhive_count=%s\nhosts=%s\n\n' \
  "$CHECK_ONLY" "$RESTART" "$FIX_HIVE_JDK_SPLIT" "$WITH_HIVE_COUNT" "${HOSTS[*]}"

run_p0_module() {
  local module="$1"
  local extra="${2:-}"
  local log="$P2_RUN_DIR/p0_${module}.log"
  p2_run_logged "p0_${module}" "P0" "$log" \
    "P0 module failed after startup; inspect the component log named in the output." \
    "cd $P2_PROJECT_ROOT && bin/p0_cluster_health_check.sh --module $module $extra" \
    bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_cluster_health_check.sh --module '$module' $extra"
}

fix_hive_jdk_split() {
  local log="$P2_RUN_DIR/fix_hive_jdk_split.log"
  local cmd
  cmd=$(cat <<'CMD'
set -euo pipefail
ts=$(date '+%Y%m%d_%H%M%S')
hive_env=/export/server/hive/conf/hive-env.sh
mapred=/export/server/hive/conf/hadoop-conf-jdk8/mapred-site.xml
hadoop_env=/export/server/hive/conf/hadoop-conf-jdk8/hadoop-env.sh
cp -p "$hive_env" "$hive_env.bak_$ts"
cp -p "$mapred" "$mapred.bak_$ts"
cp -p "$hadoop_env" "$hadoop_env.bak_$ts"
python3 - "$hive_env" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
desired = {
    "JAVA_HOME": "/export/server/jdk8",
    "HADOOP_CONF_DIR": "/export/server/hive/conf/hadoop-conf-jdk8",
    "YARN_CONF_DIR": "/export/server/hive/conf/hadoop-conf-jdk8",
    "HIVE_CONF_DIR": "/export/server/hive/conf",
}
lines = path.read_text().splitlines()
kept = []
seen = set()
for line in lines:
    stripped = line.strip()
    matched = False
    for key in desired:
        if stripped.startswith(f"export {key}=") or stripped.startswith(f"{key}="):
            if key not in seen:
                kept.append(f"export {key}={desired[key]}")
                seen.add(key)
            matched = True
            break
    if not matched:
        kept.append(line)
for key, value in desired.items():
    if key not in seen:
        kept.append(f"export {key}={value}")
path.write_text("\n".join(kept) + "\n")
PY
sed -i '/--add-opens=/d; /--add-exports=/d' "$hadoop_env"
python3 - "$mapred" <<'PY'
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

path = Path(sys.argv[1])
tree = ET.parse(path)
root = tree.getroot()
desired = {
    "mapreduce.jvm.add-opens-as-default": "false",
    "yarn.app.mapreduce.am.env": "JAVA_HOME=/export/server/jdk8,HADOOP_MAPRED_HOME=/export/server/hadoop",
    "mapreduce.map.env": "JAVA_HOME=/export/server/jdk8,HADOOP_MAPRED_HOME=/export/server/hadoop",
    "mapreduce.reduce.env": "JAVA_HOME=/export/server/jdk8,HADOOP_MAPRED_HOME=/export/server/hadoop",
    "mapred.child.env": "JAVA_HOME=/export/server/jdk8,HADOOP_MAPRED_HOME=/export/server/hadoop",
}
props = {}
for prop in list(root.findall("property")):
    name = (prop.findtext("name") or "").strip()
    if name in props:
        root.remove(prop)
        continue
    props[name] = prop
for key, value in desired.items():
    prop = props.get(key)
    if prop is None:
        prop = ET.SubElement(root, "property")
        ET.SubElement(prop, "name").text = key
        ET.SubElement(prop, "value").text = value
    else:
        value_node = prop.find("value")
        if value_node is None:
            value_node = ET.SubElement(prop, "value")
        value_node.text = value
ET.indent(tree, space="  ")
tree.write(path, encoding="unicode", xml_declaration=True)
PY
CMD
)
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "fix_hive_jdk_split" "check-only mode"
    return 0
  fi
  p2_run_logged "fix_hive_jdk_split" "Hive" "$log" \
    "Hive JDK split patch failed; compare the .bak timestamped files before retrying." \
    "cd $P2_PROJECT_ROOT && bin/p0_config_drift_check.sh" \
    bash -lc "$cmd"
}

start_hdfs_yarn() {
  local log="$P2_RUN_DIR/start_hdfs_yarn.log"
  local cmd
  cmd=$(cat <<'CMD'
set -uo pipefail
export JAVA_HOME=/export/server/jdk17
export PATH=$JAVA_HOME/bin:/export/server/hadoop/bin:/export/server/hadoop/sbin:$PATH
if jps -l | grep -q 'org.apache.hadoop.hdfs.server.namenode.NameNode' && jps -l | grep -q 'org.apache.hadoop.yarn.server.resourcemanager.ResourceManager'; then
  echo "HDFS/YARN already running"
  exit 0
fi
start-dfs.sh
start-yarn.sh
CMD
)
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_hdfs_yarn" "check-only mode"
    return 0
  fi
  p2_run_logged "start_hdfs_yarn" "HDFS/YARN" "$log" \
    "HDFS/YARN failed to start; inspect /export/server/hadoop/logs." \
    "tail -n 120 /export/server/hadoop/logs/*namenode*.log; yarn node -list -all" \
    bash -lc "$cmd"
}

start_postgresql() {
  local log="$P2_RUN_DIR/start_postgresql.log"
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_postgresql" "check-only mode"
    return 0
  fi
  p2_run_logged "start_postgresql" "PostgreSQL" "$log" \
    "PostgreSQL failed to start; inspect systemd status and PostgreSQL logs." \
    "sudo systemctl status postgresql postgresql-15 --no-pager; journalctl -u postgresql-15 --no-pager -n 100" \
    bash -lc "if ss -lntp 2>/dev/null | grep -qE '[:.]5432[[:space:]]'; then echo 'PostgreSQL already running'; else sudo systemctl start postgresql 2>/dev/null || sudo systemctl start postgresql-15; fi"
}

start_hive() {
  local log="$P2_RUN_DIR/start_hive.log"
  local cmd
  cmd=$(cat <<'CMD'
set -uo pipefail
mkdir -p /export/logs/hive
export JAVA_HOME=/export/server/jdk8
export PATH=$JAVA_HOME/bin:/export/server/hive/bin:/export/server/hadoop/bin:/export/server/hadoop/sbin:$PATH
export HADOOP_HOME=/export/server/hadoop
export HIVE_HOME=/export/server/hive
export HADOOP_CONF_DIR=/export/server/hive/conf/hadoop-conf-jdk8
export YARN_CONF_DIR=/export/server/hive/conf/hadoop-conf-jdk8
export HIVE_CONF_DIR=/export/server/hive/conf
HADOOP_CP=$(JAVA_HOME=/export/server/jdk8 /export/server/hadoop/bin/hadoop --config /export/server/hive/conf/hadoop-conf-jdk8 classpath --glob)
HIVE_CP="/export/server/hive/conf:/export/server/hive/conf/hadoop-conf-jdk8:/export/server/hive/lib/*:${HADOOP_CP}"
if ! jps -l | grep -q 'org.apache.hadoop.hive.metastore.HiveMetaStore'; then
  nohup /export/server/jdk8/bin/java -Xmx512m -Dhive.log.dir=/export/logs/hive -Dhive.log.file=hive-metastore.log -Dhadoop.log.dir=/export/logs/hive -Dhadoop.log.file=hive-metastore.log -cp "$HIVE_CP" org.apache.hadoop.hive.metastore.HiveMetaStore > /export/logs/hive/hive-metastore.out 2>&1 &
  sleep 10
else
  echo "Hive Metastore already running"
fi
if ! jps -l | grep -q 'org.apache.hive.service.server.HiveServer2'; then
  nohup /export/server/jdk8/bin/java -Xmx512m -Dhive.log.dir=/export/logs/hive -Dhive.log.file=hiveserver2.log -Dhadoop.log.dir=/export/logs/hive -Dhadoop.log.file=hiveserver2.log -cp "$HIVE_CP" org.apache.hive.service.server.HiveServer2 > /export/logs/hive/hiveserver2.out 2>&1 &
  sleep 15
else
  echo "HiveServer2 already running"
fi
jps -l | egrep 'HiveMetaStore|HiveServer2' || true
ss -lntp | egrep '9083|10000' || true
CMD
)
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_hive" "check-only mode"
    return 0
  fi
  p2_run_logged "start_hive" "Hive" "$log" \
    "Hive Metastore or HiveServer2 failed to start; inspect Hive out files." \
    "tail -n 120 /export/logs/hive/hive-metastore.out; tail -n 120 /export/logs/hive/hiveserver2.out" \
    bash -lc "$cmd"
}

start_kafka() {
  local host
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_kafka" "check-only mode"
    return 0
  fi
  for host in "${HOSTS[@]}"; do
    local log="$P2_RUN_DIR/start_kafka_${host}.log"
    local cmd="if jps -l 2>/dev/null | grep -q 'kafka.Kafka' && ss -lntp 2>/dev/null | grep -qE '[:.]9092[[:space:]]'; then echo 'Kafka already running'; else export JAVA_HOME=/export/server/jdk17; export PATH=/usr/local/bin:/usr/bin:/bin:\$JAVA_HOME/bin:\$PATH; mkdir -p /export/logs/kafka; setsid /export/server/kafka/bin/kafka-server-start.sh /export/server/kafka/config/kraft/server.properties > /export/logs/kafka/kafka-server.out 2>&1 < /dev/null & fi"
    p2_run_logged "start_kafka_${host}" "Kafka" "$log" \
      "Kafka failed to start on $host." \
      "ssh common@$host 'jps -l; ss -lntp | egrep \"9092|9093\"; tail -n 120 /export/logs/kafka/kafka-server.out'" \
      p2_run_on "$host" "$cmd"
  done
}

start_redis() {
  local log="$P2_RUN_DIR/start_redis.log"
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_redis" "check-only mode"
    return 0
  fi
  p2_run_logged "start_redis" "Redis" "$log" \
    "Redis failed to start; inspect systemd status." \
    "sudo systemctl status redis --no-pager; journalctl -u redis --no-pager -n 100" \
    bash -lc "if redis-cli -h hadoop1 ping 2>/dev/null | grep -q PONG; then echo 'Redis already running'; else sudo systemctl start redis; fi"
}

start_flink() {
  local log="$P2_RUN_DIR/start_flink.log"
  if ((CHECK_ONLY == 1)); then
    p2_report SKIP "start_flink" "check-only mode"
    return 0
  fi
  p2_run_logged "start_flink" "Flink" "$log" \
    "Flink failed to start; inspect /export/server/flink/log." \
    "/export/server/flink/bin/flink list; tail -n 120 /export/server/flink/log/*" \
    bash -lc "if ss -lntp 2>/dev/null | grep -qE '[:.]8081[[:space:]]' && jps -l 2>/dev/null | grep -q 'org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint'; then echo 'Flink already running'; else export JAVA_HOME=/export/server/jdk17; export FLINK_HOME=/export/server/flink; /export/server/flink/bin/start-cluster.sh; fi"
}

restart_base_services() {
  local host
  if ((RESTART == 0 || CHECK_ONLY == 1)); then
    return 0
  fi
  p2_run_logged "restart_stop_flink" "Flink" "$P2_RUN_DIR/restart_stop_flink.log" \
    "Flink stop command returned non-zero; inspect Flink logs before restart." \
    "tail -n 120 /export/server/flink/log/*" \
    bash -lc "/export/server/flink/bin/stop-cluster.sh 2>/dev/null || true"

  for host in "${HOSTS[@]}"; do
    p2_run_logged "restart_stop_kafka_${host}" "Kafka" "$P2_RUN_DIR/restart_stop_kafka_${host}.log" \
      "Kafka stop command returned non-zero on $host." \
      "ssh common@$host 'jps -l | grep kafka; tail -n 120 /export/logs/kafka/kafka-server.out'" \
      p2_run_on "$host" "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-server-stop.sh 2>/dev/null || true"
  done

  p2_run_logged "restart_stop_hive" "Hive" "$P2_RUN_DIR/restart_stop_hive.log" \
    "Hive stop command returned non-zero." \
    "jps -l | egrep 'HiveMetaStore|HiveServer2'; tail -n 120 /export/logs/hive/hiveserver2.out" \
    bash -lc "pkill -f 'org.apache.hive.service.server.HiveServer2|hive --service hiveserver2|RunJar.*HiveServer2' 2>/dev/null || true; pkill -f 'HiveMetaStore|hive --service metastore|RunJar.*HiveMetaStore' 2>/dev/null || true"

  p2_run_logged "restart_stop_redis" "Redis" "$P2_RUN_DIR/restart_stop_redis.log" \
    "Redis stop command returned non-zero." \
    "sudo systemctl status redis --no-pager" \
    bash -lc "sudo systemctl stop redis 2>/dev/null || true"

  p2_run_logged "restart_stop_hdfs_yarn" "HDFS/YARN" "$P2_RUN_DIR/restart_stop_hdfs_yarn.log" \
    "HDFS/YARN stop command returned non-zero." \
    "tail -n 120 /export/server/hadoop/logs/*resourcemanager*.log" \
    bash -lc "export JAVA_HOME=/export/server/jdk17; export PATH=\$JAVA_HOME/bin:/export/server/hadoop/bin:/export/server/hadoop/sbin:\$PATH; stop-yarn.sh 2>/dev/null || true; stop-dfs.sh 2>/dev/null || true"
}

if ((FIX_HIVE_JDK_SPLIT == 1)); then
  fix_hive_jdk_split
fi

restart_base_services

start_hdfs_yarn
start_postgresql
start_hive
start_kafka
start_redis
start_flink

hive_extra="--skip-hive-count"
if ((WITH_HIVE_COUNT == 1)); then
  hive_extra=""
fi

run_p0_module "hdfs-yarn"
run_p0_module "hive" "$hive_extra"
run_p0_module "kafka"
run_p0_module "redis-flink"

p2_finish

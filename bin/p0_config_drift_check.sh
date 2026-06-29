#!/usr/bin/env bash
set -uo pipefail

HOSTS=(${P0_HOSTS:-hadoop1 hadoop2 hadoop3})
KAFKA_CONF="${P0_KAFKA_CONF:-/export/server/kafka/config/kraft/server.properties}"
PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

usage() {
  cat <<'USAGE'
Usage:
  bin/p0_config_drift_check.sh [--hosts "hadoop1 hadoop2 hadoop3"]

Environment overrides:
  P0_HOSTS    Space-separated host list, default: hadoop1 hadoop2 hadoop3
  P0_KAFKA_CONF
              Kafka KRaft config path, default: /export/server/kafka/config/kraft/server.properties

Purpose:
  Read-only drift check for JDK, Hadoop, Hive, Spark, Flink, Kafka, Trino and Doris key configs.
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

check_reachability() {
  local host
  for host in "${HOSTS[@]}"; do
    local out
    if out="$(run_on "$host" 'printf "%s user=%s\n" "$(hostname -s)" "$(whoami)"' 2>&1)"; then
      report PASS "$host reachable: $out"
    else
      report FAIL "$host unreachable or keyless ssh failed: ${out//$'\n'/ }"
    fi
  done
}

check_jdks() {
  local host
  for host in "${HOSTS[@]}"; do
    local j8 j17 profile
    j8="$(run_on "$host" '/export/server/jdk8/bin/java -version 2>&1 | head -n 1' 2>&1 || true)"
    if [[ "$j8" == *'1.8.0'* || "$j8" == *'version "8.'* ]]; then
      report PASS "$host JDK8 ok: $j8"
    else
      report FAIL "$host JDK8 unexpected: ${j8:-missing}"
    fi

    j17="$(run_on "$host" '/export/server/jdk17/bin/java -version 2>&1 | head -n 1' 2>&1 || true)"
    if [[ "$j17" == *'17.'* || "$j17" == *'version "17'* ]]; then
      report PASS "$host JDK17 ok: $j17"
    else
      report FAIL "$host JDK17 unexpected: ${j17:-missing}"
    fi

    profile="$(run_on "$host" 'grep -hE "^(export )?(JAVA_HOME|HIVE_JAVA_HOME)=" /etc/profile.d/bigdata.sh ~/.bashrc 2>/dev/null || true' 2>&1 || true)"
    if [[ "$profile" == *'JAVA_HOME=/export/server/jdk17'* && "$profile" == *'HIVE_JAVA_HOME=/export/server/jdk8'* ]]; then
      report PASS "$host shell JDK split ok"
    else
      report WARN "$host shell JDK split needs review: ${profile//$'\n'/; }"
    fi
  done
}

compare_same_hash() {
  local label="$1"
  local required="$2"
  local path="$3"
  local -A seen=()
  local missing=()
  local host

  for host in "${HOSTS[@]}"; do
    local out hash
    out="$(run_on "$host" "if [[ -f '$path' ]]; then sha256sum '$path' | awk '{print \$1}'; else echo MISSING; fi" 2>&1 || true)"
    hash="$(printf '%s' "$out" | tail -n 1)"
    if [[ "$hash" == "MISSING" || -z "$hash" ]]; then
      missing+=("$host")
    else
      seen["$hash"]+="$host "
    fi
  done

  if ((${#missing[@]} == ${#HOSTS[@]})); then
    if [[ "$required" == "required" ]]; then
      report FAIL "$label missing on all hosts: $path"
    else
      report WARN "$label absent on all hosts: $path"
    fi
    return
  fi

  if ((${#missing[@]} > 0)); then
    report FAIL "$label missing on some hosts: $path missing=${missing[*]}"
    return
  fi

  if ((${#seen[@]} == 1)); then
    report PASS "$label consistent: $path"
  else
    local detail=""
    local hash
    for hash in "${!seen[@]}"; do
      detail+=" ${hash:0:12}=>${seen[$hash]}"
    done
    report FAIL "$label hash drift: $path$detail"
  fi
}

check_xml_file() {
  local host="$1"
  local path="$2"
  local label="$3"
  local cmd out
  cmd=$(cat <<CMD
if [[ ! -f '$path' ]]; then
  echo MISSING
  exit 3
fi
python3 - '$path' <<'PY'
import collections
import sys
import xml.etree.ElementTree as ET

path = sys.argv[1]
tree = ET.parse(path)
names = []
for prop in tree.findall("property"):
    name = prop.findtext("name")
    if name:
        names.append(name.strip())
dups = sorted(k for k, v in collections.Counter(names).items() if v > 1)
if dups:
    print("DUPLICATE_PROPERTIES=" + ",".join(dups))
    sys.exit(2)
print("XML_OK properties=%d" % len(names))
PY
CMD
)
  out="$(run_on "$host" "$cmd" 2>&1 || true)"
  if [[ "$out" == *"XML_OK"* ]]; then
    report PASS "$host $label XML ok"
  elif [[ "$out" == *"MISSING"* ]]; then
    report FAIL "$host $label XML missing: $path"
  else
    report FAIL "$host $label XML invalid or duplicate properties: ${out//$'\n'/ }"
  fi
}

check_hadoop_configs() {
  local file
  for file in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml capacity-scheduler.xml hadoop-env.sh workers; do
    compare_same_hash "Hadoop" required "/export/server/hadoop/etc/hadoop/$file"
  done

  local host xml
  for host in "${HOSTS[@]}"; do
    for xml in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml capacity-scheduler.xml; do
      check_xml_file "$host" "/export/server/hadoop/etc/hadoop/$xml" "Hadoop $xml"
    done
  done
}

check_hive_config() {
  local host="${P0_HIVE_HOST:-hadoop1}"
  local cmd out
  cmd=$(cat <<'CMD'
set -u
fail=0
grep -q '^export JAVA_HOME=/export/server/jdk8$' /export/server/hive/conf/hive-env.sh || { echo "hive-env missing JAVA_HOME jdk8"; fail=1; }
grep -q '^export HADOOP_CONF_DIR=/export/server/hive/conf/hadoop-conf-jdk8$' /export/server/hive/conf/hive-env.sh || { echo "hive-env HADOOP_CONF_DIR not jdk8 conf"; fail=1; }
grep -q '^export YARN_CONF_DIR=/export/server/hive/conf/hadoop-conf-jdk8$' /export/server/hive/conf/hive-env.sh || { echo "hive-env YARN_CONF_DIR not jdk8 conf"; fail=1; }
grep -q '^export HIVE_CONF_DIR=/export/server/hive/conf$' /export/server/hive/conf/hive-env.sh || { echo "hive-env missing HIVE_CONF_DIR"; fail=1; }
if grep -qE -- '--add-opens=|--add-exports=' /export/server/hive/conf/hadoop-conf-jdk8/hadoop-env.sh; then
  echo "hadoop-conf-jdk8/hadoop-env.sh still contains JDK17 module flags"
  fail=1
fi
python3 - <<'PY'
import sys
import xml.etree.ElementTree as ET

path = "/export/server/hive/conf/hadoop-conf-jdk8/mapred-site.xml"
root = ET.parse(path).getroot()
props = {}
for prop in root.findall("property"):
    name = prop.findtext("name")
    value = prop.findtext("value")
    if name:
        props[name.strip()] = (value or "").strip()

required = {
    "mapreduce.jvm.add-opens-as-default": "false",
    "yarn.app.mapreduce.am.env": "JAVA_HOME=/export/server/jdk8",
    "mapreduce.map.env": "JAVA_HOME=/export/server/jdk8",
    "mapreduce.reduce.env": "JAVA_HOME=/export/server/jdk8",
    "mapred.child.env": "JAVA_HOME=/export/server/jdk8",
}
bad = []
for key, expected in required.items():
    value = props.get(key, "")
    if expected not in value:
        bad.append("%s=%s" % (key, value))
if bad:
    print("bad mapred-site values: " + "; ".join(bad))
    sys.exit(2)
print("mapred-site jdk8 properties ok")
PY
exit "$fail"
CMD
)
  out="$(run_on "$host" "$cmd" 2>&1 || true)"
  if [[ "$out" == *"mapred-site jdk8 properties ok"* && "$out" != *"bad mapred-site"* && "$out" != *"missing"* && "$out" != *"still contains"* ]]; then
    report PASS "$host Hive JDK8 MR config ok"
  else
    report FAIL "$host Hive JDK8 MR config drift: ${out//$'\n'/ }"
  fi

  check_xml_file "$host" "/export/server/hive/conf/hive-site.xml" "Hive hive-site.xml"
}

check_worker_hive_conf() {
  local host
  for host in "${HOSTS[@]}"; do
    [[ "$host" == "${P0_HIVE_HOST:-hadoop1}" ]] && continue
    local out
    out="$(run_on "$host" 'test -f /export/server/hive/conf/hive-site.xml && echo present || echo missing' 2>&1 || true)"
    if [[ "$out" == "present" ]]; then
      report PASS "$host Hive conf present"
    else
      report WARN "$host Hive conf missing; acceptable for Hive service isolation, review before Flink/Spark tasks need local Hive conf"
    fi
  done
}

check_spark_flink_configs() {
  local file
  for file in spark-env.sh spark-defaults.conf; do
    compare_same_hash "Spark" optional "/export/server/spark/conf/$file"
  done
  for file in flink-conf.yaml workers masters; do
    compare_same_hash "Flink" optional "/export/server/flink/conf/$file"
  done
}

check_kafka_configs() {
  local -A ids=()
  local host
  for host in "${HOSTS[@]}"; do
    local out node_id voters roles
    out="$(run_on "$host" "if [[ -f '$KAFKA_CONF' ]]; then grep -E '^(node.id|process.roles|controller.quorum.voters|listeners|advertised.listeners)=' '$KAFKA_CONF'; else echo MISSING; fi" 2>&1 || true)"
    if [[ "$out" == "MISSING" || -z "$out" ]]; then
      report WARN "$host Kafka KRaft config missing: $KAFKA_CONF"
      continue
    fi
    node_id="$(printf '%s\n' "$out" | awk -F= '$1=="node.id"{print $2}' | tail -n 1)"
    voters="$(printf '%s\n' "$out" | awk -F= '$1=="controller.quorum.voters"{print $2}' | tail -n 1)"
    roles="$(printf '%s\n' "$out" | awk -F= '$1=="process.roles"{print $2}' | tail -n 1)"
    if [[ -n "$node_id" ]]; then
      ids["$node_id"]+="$host "
      report PASS "$host Kafka node.id=$node_id roles=${roles:-missing}"
    else
      report FAIL "$host Kafka node.id missing"
    fi
    if [[ "$voters" == *"1@"*":9093"* && "$voters" == *"2@"*":9093"* && "$voters" == *"3@"*":9093"* ]]; then
      report PASS "$host Kafka quorum voters include all three hosts"
    else
      report FAIL "$host Kafka quorum voters unexpected: ${voters:-missing}"
    fi
  done

  local id duplicate=0
  for id in "${!ids[@]}"; do
    if [[ "${ids[$id]}" == *" "*[![:space:]]*" "* ]]; then
      report FAIL "Kafka duplicate node.id=$id on ${ids[$id]}"
      duplicate=1
    fi
  done
  ((duplicate == 0)) && report PASS "Kafka node.id values are unique"
}

check_trino_doris_configs() {
  local host
  for host in "${HOSTS[@]}"; do
    local trino
    trino="$(run_on "$host" "if [[ -f /export/server/trino/etc/node.properties ]]; then grep -E '^(node.id|node.environment)=' /export/server/trino/etc/node.properties; if [[ -f /export/server/trino/etc/config.properties ]]; then grep -E '^(coordinator|http-server.http.port)=' /export/server/trino/etc/config.properties; fi; else echo MISSING; fi" 2>&1 || true)"
    if [[ "$trino" == "MISSING" || -z "$trino" ]]; then
      report WARN "$host Trino config missing"
    else
      report PASS "$host Trino config present"
    fi

    local doris
    doris="$(run_on "$host" "if [[ -f /export/server/doris/fe/conf/fe.conf || -f /export/server/doris/be/conf/be.conf ]]; then echo present; else echo MISSING; fi" 2>&1 || true)"
    if [[ "$doris" == "present" ]]; then
      report PASS "$host Doris config present"
    else
      report WARN "$host Doris config missing"
    fi
  done
}

main() {
  printf 'P0 config drift check started at %s\n' "$(date '+%F %T')"
  printf 'Hosts: %s\n\n' "${HOSTS[*]}"

  check_reachability
  check_jdks
  check_hadoop_configs
  check_hive_config
  check_worker_hive_conf
  check_spark_flink_configs
  check_kafka_configs
  check_trino_doris_configs

  printf '\nSUMMARY pass=%d warn=%d fail=%d\n' "$PASS_COUNT" "$WARN_COUNT" "$FAIL_COUNT"
  if ((FAIL_COUNT > 0)); then
    exit 1
  fi
}

main "$@"

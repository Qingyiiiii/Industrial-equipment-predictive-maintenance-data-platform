#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

DRY_RUN=1
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p7_alert_rules_plan.sh [--dry-run]

Purpose:
  Generate suggested alert thresholds for the MetroPT three-node cluster.
  It does not install Prometheus, Grafana, logrotate, systemd timers, or any resident agent.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
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

p2_init "p7_alert_rules_plan" "${ORIGINAL_ARGS[@]}"
p2_header "P7 alert rules plan"
printf 'dry_run=%s\n\n' "$DRY_RUN"

RULES_TSV="$P2_RUN_DIR/alert_rules_plan.tsv"
RULES_MD="$P2_RUN_DIR/alert_rules_plan.md"
RULES_JSON="$P2_RUN_DIR/alert_rules_plan.json"

printf 'area\tmetric\twarn_threshold\tcritical_threshold\trationale\tnext_command\n' > "$RULES_TSV"

rule() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$1" "$2" "$3" "$4" "$5" "$6" >> "$RULES_TSV"
}

rule "host" "mem_available_mb" "<2048 on hadoop1 or <1024 on workers" "<1024 on hadoop1 or <512 on workers" "Avoid running offline/realtime/extended jobs without heap and container headroom." "bin/p7_ops_snapshot.sh; free -m; yarn application -list -appStates RUNNING"
rule "host" "mem_available_pct" "<15%" "<8%" "This cluster has limited memory; low available memory causes YARN, Flink, Trino and Doris instability." "bin/p7_ops_snapshot.sh; ps -eo pid,comm,%mem,rss,args --sort=-rss | head"
rule "host" "load1_per_core" ">0.70" ">1.00" "Sustained CPU saturation makes Spark/Flink validation noisy." "uptime; ps -eo pid,comm,%cpu,args --sort=-%cpu | head"
rule "disk" "filesystem_used_pct" ">80%" ">90%" "HDFS, Hive warehouse, Kafka logs and Doris data need write headroom." "df -h; du -sh /export/* /home/common/tmp/* 2>/dev/null"
rule "logs" "component_log_dir_size" ">2GB" ">5GB" "Long-running Hadoop/Hive/Flink/Kafka/Doris logs should be reviewed before they fill disks." "bin/p2_log_maintenance_plan.sh --dry-run"
rule "hdfs_yarn" "running_yarn_nodes" "<3" "<2" "Offline and realtime modes expect all three NodeManagers available." "bin/p0_cluster_health_check.sh --module hdfs-yarn; yarn node -list -all"
rule "hdfs_yarn" "running_yarn_apps" ">0 before acceptance" ">2 before acceptance" "Acceptance runs should start from a quiet YARN queue unless intentionally testing concurrency." "yarn application -list -appStates RUNNING"
rule "hive" "metastore_or_hs2_port" "9083 or 10000 down" "both down" "Hive publish, realtime table checks and Hive COUNT fallback depend on these services." "bin/start_base_services.sh; tail -n 120 /export/logs/hive/*.out"
rule "kafka" "broker_controller_ports" "any 9092/9093 down" "quorum CLI fails" "Kafka KRaft health needs process, listener, quorum and topic CLI checks together." "export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller hadoop1:9093 describe --status"
rule "flink" "jobmanager_or_taskmanager" "8081 down or TaskManager missing" "JobManager unreachable" "Realtime demo needs Flink session cluster even if no long job remains running." "bin/p0_cluster_health_check.sh --module redis-flink; /export/server/flink/bin/flink list"
rule "redis" "redis_ping" "PONG missing" "6379 down" "Realtime KPI evidence depends on Redis key visibility." "redis-cli -h hadoop1 ping; redis-cli -h hadoop1 --scan --pattern 'metropt:kpi:1m:*' | head"
rule "trino" "coordinator_select1" "8080 down when Trino mode requested" "SELECT 1 fails when Trino mode requested" "Trino remains optional extended mode but should be explicit before query comparison." "bin/start_extended_query_mode.sh --trino-only; bin/p2_query_perf_compare.sh --engine hive,trino --query-set smoke"
rule "doris" "alive_backends" "<3 when Doris mode requested" "9030 down or SHOW BACKENDS fails" "Doris health is FE MySQL plus SHOW BACKENDS Alive=true, not port-only." "bin/start_extended_query_mode.sh --doris-only --allow-swapoff; mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW BACKENDS;'"

{
  cat <<EOF
# P7 Alert Rules Plan

- run_id: $P2_RUN_ID
- generated_at: $(date '+%F %T')
- dry_run: $DRY_RUN
- install_action: none

This is a threshold plan only. It does not install Prometheus, Grafana, systemd timers, logrotate, or resident agents.

| Area | Metric | Warn | Critical | Rationale | Next Command |
| --- | --- | --- | --- | --- | --- |
EOF
  awk -F'\t' 'NR>1 {for (i=1;i<=NF;i++) gsub(/\|/,"/",$i); printf "| `%s` | `%s` | %s | %s | %s | `%s` |\n", $1,$2,$3,$4,$5,$6}' "$RULES_TSV"
} > "$RULES_MD"

python3 - "$RULES_JSON" "$P2_RUN_ID" "$RULES_TSV" <<'PY'
import csv
import json
import sys
from pathlib import Path

out, run_id, rules_tsv = sys.argv[1:]
with open(rules_tsv, "r", encoding="utf-8", newline="") as f:
    rules = list(csv.DictReader(f, delimiter="\t"))
payload = {
    "phase": "P7",
    "run_id": run_id,
    "dry_run": True,
    "install_action": "none",
    "rules": rules,
}
Path(out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

p2_report PASS "alert_rules_plan_tsv" "rules=$RULES_TSV"
p2_report PASS "alert_rules_plan_md" "rules=$RULES_MD"
p2_report PASS "alert_rules_plan_json" "rules=$RULES_JSON"

printf '\nalert_rules_plan_md=%s\nalert_rules_plan_json=%s\nalert_rules_plan_tsv=%s\n' \
  "$RULES_MD" "$RULES_JSON" "$RULES_TSV"
p2_finish

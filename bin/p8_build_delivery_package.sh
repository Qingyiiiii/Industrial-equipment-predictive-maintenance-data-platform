#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

OUTPUT_ROOT=""
PACKAGE_NAME=""
BUILD_INDEX=1
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p8_build_delivery_package.sh [options]

Options:
  --output-root PATH     Delivery package output root. Default: data/metropt_quality/delivery_packages.
  --package-name NAME    Package directory name. Default: p8_delivery_package_<run_id>.
  --no-index             Do not generate delivery_index.md.
  -h, --help             Show help.

Purpose:
  Build a final delivery package from the latest P3/P4/P5/P6/P7 evidence.
  It only writes compact markdown/json/tsv manifests and references existing run directories.
  It does not copy large CSV, Parquet, HDFS data, logs, jars, or service data.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-root)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --package-name)
      PACKAGE_NAME="${2:-}"
      shift 2
      ;;
    --no-index)
      BUILD_INDEX=0
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

p2_init "p8_delivery_package" "${ORIGINAL_ARGS[@]}"
p2_header "P8 delivery package"

VALIDATION_ROOT="$P2_PROJECT_ROOT/data/metropt_quality/validation_runs"
DELIVERY_REPORT_ROOT="$P2_PROJECT_ROOT/data/metropt_quality/delivery_reports"
OUTPUT_ROOT="${OUTPUT_ROOT:-$P2_PROJECT_ROOT/data/metropt_quality/delivery_packages}"
PACKAGE_NAME="${PACKAGE_NAME:-p8_delivery_package_${P2_RUN_ID}}"
PACKAGE_DIR="$OUTPUT_ROOT/$PACKAGE_NAME"
mkdir -p "$PACKAGE_DIR"

MANIFEST="$PACKAGE_DIR/evidence_manifest.tsv"
PACKAGE_JSON="$PACKAGE_DIR/delivery_package.json"
PACKAGE_SUMMARY="$PACKAGE_DIR/package_summary.md"
PROJECT_OVERVIEW="$PACKAGE_DIR/project_overview.md"
RUN_ORDER="$PACKAGE_DIR/run_order.md"
ACCEPTANCE_RESULTS="$PACKAGE_DIR/acceptance_results.md"
METRICS_AND_QUERIES="$PACKAGE_DIR/metrics_and_queries.md"
REALTIME_DEMO="$PACKAGE_DIR/realtime_demo_steps.md"
TROUBLESHOOTING="$PACKAGE_DIR/troubleshooting_entry.md"
DELIVERY_INDEX="$PACKAGE_DIR/delivery_index.md"

printf 'phase\tartifact\tstatus\tpath\tsummary\n' > "$MANIFEST"

latest_dir() {
  local root="$1"
  local pattern="$2"
  ls -1dt "$root"/$pattern 2>/dev/null | head -n 1 || true
}

read_tsv_value() {
  local file="$1"
  local key="$2"
  awk -F'\t' -v key="$key" '$1==key {value=$2} END{print value}' "$file" 2>/dev/null || true
}

extract_nested_run_dir() {
  local log="$1"
  grep -m1 '^run_dir=' "$log" 2>/dev/null | cut -d= -f2- || true
}

summary_from_command_tsv() {
  local command_tsv="$1"
  if [[ ! -f "$command_tsv" ]]; then
    printf 'missing command.tsv'
    return
  fi
  local pass warn skip fail rc
  pass="$(awk -F'\t' '$1=="pass"{print $2}' "$command_tsv" | tail -n 1)"
  warn="$(awk -F'\t' '$1=="warn"{print $2}' "$command_tsv" | tail -n 1)"
  skip="$(awk -F'\t' '$1=="skip"{print $2}' "$command_tsv" | tail -n 1)"
  fail="$(awk -F'\t' '$1=="fail"{print $2}' "$command_tsv" | tail -n 1)"
  rc="$(awk -F'\t' '$1=="return_code"{print $2}' "$command_tsv" | tail -n 1)"
  if [[ -n "$pass$warn$skip$fail$rc" ]]; then
    printf 'pass=%s warn=%s skip=%s fail=%s rc=%s' \
      "${pass:-0}" "${warn:-0}" "${skip:-0}" "${fail:-0}" "${rc:-N/A}"
  else
    printf 'command summary missing'
  fi
}

status_from_command_tsv() {
  local command_tsv="$1"
  if [[ ! -f "$command_tsv" ]]; then
    printf 'MISSING'
    return
  fi
  local warn fail rc
  warn="$(awk -F'\t' '$1=="warn"{print $2}' "$command_tsv" | tail -n 1)"
  fail="$(awk -F'\t' '$1=="fail"{print $2}' "$command_tsv" | tail -n 1)"
  rc="$(awk -F'\t' '$1=="return_code"{print $2}' "$command_tsv" | tail -n 1)"
  if [[ "${fail:-0}" != "0" || "${rc:-0}" != "0" ]]; then
    printf 'FAIL'
  elif [[ "${warn:-0}" != "0" ]]; then
    printf 'WARN'
  else
    printf 'PASS'
  fi
}

status_from_readiness_tsv() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    printf 'MISSING'
    return
  fi
  awk -F'\t' '
    NR>1 {
      total++
      if ($2=="NOT_READY") not_ready++
      else if ($2=="WARN") warn++
      else if ($2=="NOT_RUNNING") not_running++
    }
    END {
      if (not_ready > 0) print "FAIL"
      else if (warn > 0) print "WARN"
      else if (not_running > 0) print "SKIP"
      else if (total > 0) print "PASS"
      else print "MISSING"
    }
  ' "$file"
}

clean_field() {
  local value="${1:-}"
  value="${value//$'\t'/ }"
  value="${value//$'\r'/ }"
  value="${value//$'\n'/ }"
  printf '%s' "$value"
}

manifest_line() {
  local phase="$1"
  local artifact="$2"
  local status="$3"
  local path="$4"
  local summary="$5"
  printf '%s\t%s\t%s\t%s\t%s\n' \
    "$phase" "$artifact" "$status" "$(clean_field "$path")" "$(clean_field "$summary")" >> "$MANIFEST"
}

markdown_from_tsv() {
  local file="$1"
  local columns="$2"
  if [[ ! -f "$file" ]]; then
    printf 'Missing `%s`.\n\n' "$file"
    return
  fi
  awk -F'\t' -v columns="$columns" '
    BEGIN {
      n=split(columns, names, ",")
      printf "|"
      for (i=1; i<=n; i++) printf " %s |", names[i]
      printf "\n|"
      for (i=1; i<=n; i++) printf " --- |"
      printf "\n"
    }
    NR>1 {
      printf "|"
      for (i=1; i<=n; i++) {
        value=$i
        gsub(/\|/, "/", value)
        printf " %s |", value
      }
      printf "\n"
    }
  ' "$file"
  printf '\n'
}

P3_RUN="$(latest_dir "$VALIDATION_ROOT" 'p3_project_delivery_*')"
P4_RUN="$(latest_dir "$VALIDATION_ROOT" 'p4_delivery_report_*')"
P4_REPORT_DIR="$(latest_dir "$DELIVERY_REPORT_ROOT" 'p4_delivery_*')"
P5_RUN="$(latest_dir "$VALIDATION_ROOT" 'p5_doris_acceptance_*')"
P6_RUN="$(latest_dir "$VALIDATION_ROOT" 'p6_realtime_demo_*')"
P7_RUN="$(latest_dir "$VALIDATION_ROOT" 'p7_ops_snapshot_*')"
P7_ALERT_RUN="$(latest_dir "$VALIDATION_ROOT" 'p7_alert_rules_plan_*')"
QUERY_RUN="$(latest_dir "$VALIDATION_ROOT" 'query_perf_compare_*')"

P4_REPORT_MD="$P4_REPORT_DIR/delivery_report.md"
P4_STATUS_TSV="$P4_REPORT_DIR/current_status.tsv"
P6_STATUS="$P6_RUN/demo_status.tsv"
P7_READINESS="$P7_RUN/readiness.tsv"
P7_HOST_METRICS="$P7_RUN/host_metrics.tsv"
P7_SERVICE_STATUS="$P7_RUN/service_status.tsv"
QUERY_RESULTS="$QUERY_RUN/query_perf_results.tsv"
P3_DQ_RUN=""
if [[ -n "$P3_RUN" ]]; then
  P3_DQ_RUN="$(extract_nested_run_dir "$P3_RUN/p3_data_quality.log")"
fi
P3_DQ_METRICS="$P3_DQ_RUN/data_quality_metrics.tsv"

missing_required=0
require_path() {
  local label="$1"
  local path="$2"
  if [[ -n "$path" && -e "$path" ]]; then
    p2_report PASS "evidence_${label}" "path=$path"
  else
    p2_report FAIL "evidence_${label}" "required evidence missing: ${path:-N/A}"
    missing_required=1
  fi
}

require_path "p3_project_delivery" "$P3_RUN"
require_path "p4_delivery_report" "$P4_REPORT_MD"
require_path "p5_doris_acceptance" "$P5_RUN"
require_path "p6_realtime_demo" "$P6_STATUS"
require_path "p7_ops_snapshot" "$P7_READINESS"
require_path "query_perf_results" "$QUERY_RESULTS"

manifest_line "P3" "Project delivery acceptance" "$(status_from_command_tsv "$P3_RUN/command.tsv")" "$P3_RUN" "$(summary_from_command_tsv "$P3_RUN/command.tsv")"
manifest_line "P4" "Delivery report" "$(status_from_command_tsv "$P4_RUN/command.tsv")" "$P4_REPORT_MD" "$(summary_from_command_tsv "$P4_RUN/command.tsv")"
manifest_line "P5" "Doris extended query closure" "$(status_from_command_tsv "$P5_RUN/command.tsv")" "$P5_RUN" "$(summary_from_command_tsv "$P5_RUN/command.tsv")"
manifest_line "P6" "Realtime demo evidence" "$(status_from_command_tsv "$P6_RUN/command.tsv")" "$P6_RUN" "$(summary_from_command_tsv "$P6_RUN/command.tsv")"
manifest_line "P7" "Ops snapshot" "$(status_from_readiness_tsv "$P7_READINESS")" "$P7_RUN" "$(awk -F'\t' 'NR>1 {total++; count[$2]++} END{printf "total=%d ready=%d warn=%d not_ready=%d not_running=%d", total+0, count["READY"]+0, count["WARN"]+0, count["NOT_READY"]+0, count["NOT_RUNNING"]+0}' "$P7_READINESS" 2>/dev/null || true)"
manifest_line "P7" "Alert rule dry-run plan" "$(status_from_command_tsv "$P7_ALERT_RUN/command.tsv")" "$P7_ALERT_RUN" "$(summary_from_command_tsv "$P7_ALERT_RUN/command.tsv")"
manifest_line "Query" "Hive Trino Doris smoke comparison" "$(status_from_command_tsv "$QUERY_RUN/command.tsv")" "$QUERY_RESULTS" "$(summary_from_command_tsv "$QUERY_RUN/command.tsv")"

write_project_overview() {
  cat > "$PROJECT_OVERVIEW" <<EOF
# Project Overview

## Scope

- Project: MetroPT-3 industrial air-compressor data platform.
- Business domain: \`metropt_quality\`.
- Remote project root: \`$P2_PROJECT_ROOT\`.
- Cluster config: \`$P2_PROJECT_ROOT/config/metropt_quality.cluster.yaml\`.
- Delivery package: \`$PACKAGE_DIR\`.

## Dataset And Storage References

No raw data or Parquet datasets are copied into this package. Use the following references:

| Layer | Path |
| --- | --- |
| Raw CSV | \`hdfs:///lakehouse/projects/metropt_quality/raw/MetroPT3_AirCompressor.csv\` |
| Profile | \`hdfs:///lakehouse/projects/metropt_quality/profile\` |
| ODS Parquet | \`hdfs:///lakehouse/projects/metropt_quality/ods/readings\` |
| DWD sensor long | \`hdfs:///lakehouse/projects/metropt_quality/dwd/sensor_long\` |
| DWS overall KPI | \`hdfs:///lakehouse/projects/metropt_quality/dws/overall_kpi\` |
| DWS window KPI | \`hdfs:///lakehouse/projects/metropt_quality/dws/window_kpi\` |
| DWS sensor KPI | \`hdfs:///lakehouse/projects/metropt_quality/dws/sensor_kpi\` |
| Hive database | \`metropt_quality\` |
| Iceberg database | \`metropt_quality_iceberg\` |
| Doris database | \`metropt_quality_olap\` |
| Kafka topic | \`metropt.ods.compressor.reading.v1\` |
| Kafka DLQ topic | \`metropt.ods.compressor.reading.dlq.v1\` |
| Redis KPI key pattern | \`metropt:kpi:1m:*\` |

## Main Tables

| Engine | Objects |
| --- | --- |
| Hive | \`ods_metropt_readings\`, \`dwd_metropt_sensor_long\`, \`dws_metropt_window_kpi\`, \`dws_metropt_sensor_kpi\`, realtime ODS/KPI tables and BI views |
| Trino/Iceberg | \`lakehouse.metropt_quality_iceberg\` catalog/database checks from query comparison |
| Doris | \`metropt_quality_olap.dws_metropt_sensor_kpi\` |
EOF
}

write_run_order() {
  cat > "$RUN_ORDER" <<'EOF'
# Run Order

## 1. One-command Ops Snapshot

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh
```

Use this first to decide whether the cluster is ready for offline, realtime, Trino, or Doris mode.

## 2. Base Service Check

```bash
bin/start_base_services.sh --check-only
bin/p0_cluster_health_check.sh --module basic
```

## 3. Offline Acceptance

```bash
bin/p1_metropt_offline_acceptance.sh
```

If P7 reports low memory for offline mode, stop optional extended services first and rerun the snapshot.

## 4. Realtime Demo

```bash
bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500
```

This is a bounded demo. A stopped Flink job after successful processing is not a realtime failure.

## 5. Query Comparison

```bash
bin/p2_query_perf_compare.sh --engine hive,trino,doris --query-set smoke
```

Trino and Doris are extended query modes. Start them only when needed.

## 6. Final Report And Package

```bash
bin/p4_delivery_report.sh
bin/p8_build_delivery_package.sh
```
EOF
}

write_acceptance_results() {
  cat > "$ACCEPTANCE_RESULTS" <<EOF
# Acceptance Results

## Evidence Manifest

EOF
  markdown_from_tsv "$MANIFEST" "Phase,Artifact,Status,Path,Summary" >> "$ACCEPTANCE_RESULTS"

  cat >> "$ACCEPTANCE_RESULTS" <<EOF
## P4 Current Status

EOF
  if [[ -f "$P4_STATUS_TSV" ]]; then
    markdown_from_tsv "$P4_STATUS_TSV" "Component,Status,Detail" >> "$ACCEPTANCE_RESULTS"
  else
    printf 'P4 current status file missing: `%s`\n\n' "$P4_STATUS_TSV" >> "$ACCEPTANCE_RESULTS"
  fi

  cat >> "$ACCEPTANCE_RESULTS" <<EOF
## P7 Mode Readiness

EOF
  markdown_from_tsv "$P7_READINESS" "Mode,Status,Reason,Next Command" >> "$ACCEPTANCE_RESULTS"
}

write_metrics_and_queries() {
  cat > "$METRICS_AND_QUERIES" <<EOF
# Key Metrics And Query Comparison

## Current Accepted Baselines

| Metric | Value |
| --- | ---: |
| ODS readings | 1516948 |
| DWD sensor long rows | 22754220 |
| DWS window KPI rows | 269991 |
| DWS sensor KPI rows | 15 |
| Realtime replay sent | $(read_tsv_value "$P6_STATUS" replay_sent) |
| Realtime replay failed | $(read_tsv_value "$P6_STATUS" replay_failed) |
| Redis KPI sample keys | $(read_tsv_value "$P6_STATUS" redis_kpi_key_sample_count) |
| Doris alive backends | $(awk -F'\t' '$1=="doris_query" {if (match($3,/alive_backends=[0-9]+/)) print substr($3,RSTART+15,RLENGTH-15)}' "$P7_READINESS" 2>/dev/null | tail -n 1) |

## Query Smoke Results

EOF
  if [[ -f "$QUERY_RESULTS" ]]; then
    markdown_from_tsv "$QUERY_RESULTS" "Engine,Query ID,Return Code,Seconds,Log" >> "$METRICS_AND_QUERIES"
  else
    printf 'Query results missing: `%s`\n\n' "$QUERY_RESULTS" >> "$METRICS_AND_QUERIES"
  fi

  cat >> "$METRICS_AND_QUERIES" <<EOF
## Data Quality Metrics

EOF
  if [[ -f "$P3_DQ_METRICS" ]]; then
    markdown_from_tsv "$P3_DQ_METRICS" "Result,Metric,Actual,Expected,Detail" >> "$METRICS_AND_QUERIES"
  else
    printf 'Data quality metrics not found: `%s`\n\n' "$P3_DQ_METRICS" >> "$METRICS_AND_QUERIES"
  fi
}

write_realtime_demo() {
  cat > "$REALTIME_DEMO" <<EOF
# Realtime Demo Steps

## Latest Demo Evidence

| Key | Value |
| --- | --- |
| Demo run | \`$P6_RUN\` |
| Overall status | \`$(read_tsv_value "$P6_STATUS" overall_status)\` |
| Current state | \`$(read_tsv_value "$P6_STATUS" current_state)\` |
| Flink observed state | \`$(read_tsv_value "$P6_STATUS" flink_job_observed_state)\` |
| Replay sent | \`$(read_tsv_value "$P6_STATUS" replay_sent)\` |
| Replay failed | \`$(read_tsv_value "$P6_STATUS" replay_failed)\` |
| Redis KPI sample count | \`$(read_tsv_value "$P6_STATUS" redis_kpi_key_sample_count)\` |
| Hive realtime sample | \`$(read_tsv_value "$P6_STATUS" hive_realtime_sample)\` |

## Demo Command

~~~bash
cd /home/common/tmp/pycharm_Design
bin/p6_realtime_demo_mode.sh --start --duration-minutes 1 --max-events 10000 --rate 500
~~~

## Status And Stop

~~~bash
bin/p6_realtime_demo_mode.sh --status
bin/p6_realtime_demo_mode.sh --stop
~~~

The expected final state for the bounded demo is \`current_state=not_running\` with \`overall_status=PASS\`.
That means the demo evidence is valid and no long-running Flink job is left behind.
EOF
}

write_troubleshooting() {
  cat > "$TROUBLESHOOTING" <<'EOF'
# Troubleshooting Entry

## First Command

```bash
cd /home/common/tmp/pycharm_Design
bin/p7_ops_snapshot.sh
```

Use `ops_snapshot.md` first. It contains readiness, component status, and next commands.

## Common Follow-up Commands

| Area | Command |
| --- | --- |
| HDFS/YARN | `bin/p0_cluster_health_check.sh --module hdfs-yarn; yarn node -list -all; yarn application -list -appStates RUNNING` |
| Hive | `bin/p0_cluster_health_check.sh --module hive; tail -n 120 /export/logs/hive/*.out` |
| Kafka | `export JAVA_HOME=/export/server/jdk17; /export/server/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller hadoop1:9093 describe --status` |
| Flink/Redis | `bin/p0_cluster_health_check.sh --module redis-flink; /export/server/flink/bin/flink list; redis-cli -h hadoop1 ping` |
| Trino | `bin/start_extended_query_mode.sh --trino-only; /export/server/trino/bin/launcher status` |
| Doris | `bin/p5_doris_acceptance.sh --check-only; mysql -h 127.0.0.1 -P 9030 -uroot -e 'SHOW BACKENDS;'` |
| Query comparison | `bin/p2_query_perf_compare.sh --engine hive,trino,doris --query-set smoke` |
| Realtime demo | `bin/p6_realtime_demo_mode.sh --status` |

## Project Docs

| Document | Purpose |
| --- | --- |
| `Optimize/项目优化总结.md` | P0-P8 optimization phase summary |
| `Optimize/问题排查总结.md` | Symptom, root cause, fix and verification notes |
| `通用大数据流程配置.md` | Big-data platform configuration and operation handbook |
| `MetroPT-3虚拟机测试执行清单.md` | MetroPT implementation notes |
EOF
}

write_summary_and_index() {
  local p7_summary
  p7_summary="$(awk -F'\t' 'NR>1 {total++; count[$2]++} END{printf "total=%d ready=%d warn=%d not_ready=%d not_running=%d", total+0, count["READY"]+0, count["WARN"]+0, count["NOT_READY"]+0, count["NOT_RUNNING"]+0}' "$P7_READINESS" 2>/dev/null || true)"
  cat > "$PACKAGE_SUMMARY" <<EOF
# P8 Delivery Package Summary

- run_id: $P2_RUN_ID
- generated_at: $(date '+%F %T')
- package_dir: \`$PACKAGE_DIR\`
- source_project_root: \`$P2_PROJECT_ROOT\`
- no_large_data_copied: true

## Latest Evidence

| Phase | Run / Report |
| --- | --- |
| P3 | \`$P3_RUN\` |
| P4 | \`$P4_REPORT_MD\` |
| P5 | \`$P5_RUN\` |
| P6 | \`$P6_RUN\` |
| P7 | \`$P7_RUN\` |
| P7 alert rules | \`$P7_ALERT_RUN\` |
| Query comparison | \`$QUERY_RESULTS\` |

## Current Delivery State

- P4 status: $(summary_from_command_tsv "$P4_RUN/command.tsv")
- P6 realtime demo: overall=\`$(read_tsv_value "$P6_STATUS" overall_status)\`, flink_state=\`$(read_tsv_value "$P6_STATUS" flink_job_observed_state)\`, sent=\`$(read_tsv_value "$P6_STATUS" replay_sent)\`, failed=\`$(read_tsv_value "$P6_STATUS" replay_failed)\`
- P7 readiness: $p7_summary

## Package Files

| File | Purpose |
| --- | --- |
| \`delivery_index.md\` | Final entry page |
| \`project_overview.md\` | Project scope, storage and table references |
| \`run_order.md\` | Start, validation and demo order |
| \`acceptance_results.md\` | P3-P7 evidence summary |
| \`metrics_and_queries.md\` | Core metrics and query smoke comparison |
| \`realtime_demo_steps.md\` | Demo commands and expected state |
| \`troubleshooting_entry.md\` | Troubleshooting entry points |
| \`evidence_manifest.tsv\` | Machine-readable evidence manifest |
| \`delivery_package.json\` | Structured package metadata |
EOF

  if ((BUILD_INDEX == 1)); then
    cat > "$DELIVERY_INDEX" <<EOF
# MetroPT-3 Final Delivery Index

This is the final entry page for the MetroPT-3 delivery package.

## Start Here

1. Read \`project_overview.md\`.
2. Check \`acceptance_results.md\`.
3. Use \`run_order.md\` to reproduce validation.
4. Use \`metrics_and_queries.md\` for KPI and query comparison evidence.
5. Use \`realtime_demo_steps.md\` for the realtime demo.
6. Use \`troubleshooting_entry.md\` when a component is not ready.

## Current Verdict

- P4 delivery report: \`$P4_REPORT_MD\`
- P7 ops snapshot: \`$P7_RUN/ops_snapshot.md\`
- Delivery package summary: \`$PACKAGE_SUMMARY\`

## Files

| File | Description |
| --- | --- |
| \`project_overview.md\` | Project and data references |
| \`run_order.md\` | Reproducible run order |
| \`acceptance_results.md\` | P3-P7 evidence matrix |
| \`metrics_and_queries.md\` | Metrics and query comparison |
| \`realtime_demo_steps.md\` | Realtime demo guide |
| \`troubleshooting_entry.md\` | Troubleshooting commands |
| \`evidence_manifest.tsv\` | Evidence manifest |
| \`delivery_package.json\` | Structured metadata |
EOF
  fi
}

write_package_json() {
  python3 - "$PACKAGE_JSON" "$P2_RUN_ID" "$PACKAGE_DIR" "$MANIFEST" "$P7_READINESS" "$P4_STATUS_TSV" "$P6_STATUS" "$QUERY_RESULTS" <<'PY'
import csv
import json
import sys
from pathlib import Path

out, run_id, package_dir, manifest_tsv, readiness_tsv, p4_status_tsv, p6_status_tsv, query_results_tsv = sys.argv[1:]

def read_tsv(path):
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))

def read_kv(path):
    result = {}
    p = Path(path)
    if not p.exists():
        return result
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader, None)
        for row in reader:
            if len(row) >= 2:
                result[row[0]] = row[1]
    return result

payload = {
    "phase": "P8",
    "run_id": run_id,
    "package_dir": package_dir,
    "no_large_data_copied": True,
    "evidence_manifest": read_tsv(manifest_tsv),
    "readiness": read_tsv(readiness_tsv),
    "p4_current_status": read_tsv(p4_status_tsv),
    "p6_demo_status": read_kv(p6_status_tsv),
    "query_results": read_tsv(query_results_tsv),
}
Path(out).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
}

if ((missing_required != 0)); then
  p2_failure_template "evidence_collection" "P8" 1 "$MANIFEST" "" \
    "Required delivery evidence is missing. Run the missing P3-P7 phase first." \
    "cd $P2_PROJECT_ROOT && bin/p4_delivery_report.sh && bin/p7_ops_snapshot.sh"
fi

write_project_overview
write_run_order
write_acceptance_results
write_metrics_and_queries
write_realtime_demo
write_troubleshooting
write_summary_and_index
write_package_json

p2_report PASS "delivery_package_dir" "package=$PACKAGE_DIR"
p2_report PASS "delivery_package_summary" "summary=$PACKAGE_SUMMARY"
if ((BUILD_INDEX == 1)); then
  p2_report PASS "delivery_index" "index=$DELIVERY_INDEX"
else
  p2_report SKIP "delivery_index" "disabled by --no-index"
fi
p2_report PASS "delivery_package_json" "json=$PACKAGE_JSON"

printf '\npackage_dir=%s\npackage_summary=%s\ndelivery_index=%s\nevidence_manifest=%s\ndelivery_package_json=%s\n' \
  "$PACKAGE_DIR" "$PACKAGE_SUMMARY" "${DELIVERY_INDEX:-N/A}" "$MANIFEST" "$PACKAGE_JSON"
p2_finish

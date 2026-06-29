#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

CHECK_ONLY=0
WITH_HIVE_COUNT=0
HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/start_realtime_mode.sh [--check-only] [--hive-count] [--hosts "hadoop1 hadoop2 hadoop3"]

Purpose:
  Ensure the realtime small-loop base is up: Kafka, Flink, Redis, HDFS/YARN and Hive.
  It does not submit streaming jobs and does not replay data.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only)
      CHECK_ONLY=1
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

p2_init "start_realtime_mode" "${ORIGINAL_ARGS[@]}"
p2_header "P2 realtime mode startup"

args=(--hosts "${HOSTS[*]}")
if ((CHECK_ONLY == 1)); then
  args+=(--check-only)
fi
if ((WITH_HIVE_COUNT == 1)); then
  args+=(--hive-count)
fi

p2_run_logged "ensure_base_services" "base" "$P2_RUN_DIR/ensure_base_services.log" \
  "Base service startup/check failed before realtime validation." \
  "cd $P2_PROJECT_ROOT && bin/start_base_services.sh --check-only" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/start_base_services.sh ${args[*]@Q}"

p2_run_logged "p0_kafka" "Kafka" "$P2_RUN_DIR/p0_kafka.log" \
  "Kafka is not ready for replay." \
  "cd $P2_PROJECT_ROOT && bin/p0_cluster_health_check.sh --module kafka" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_cluster_health_check.sh --module kafka"

p2_run_logged "p0_redis_flink" "Redis/Flink" "$P2_RUN_DIR/p0_redis_flink.log" \
  "Redis or Flink is not ready for realtime validation." \
  "cd $P2_PROJECT_ROOT && bin/p0_cluster_health_check.sh --module redis-flink" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_cluster_health_check.sh --module redis-flink"

hive_extra="--skip-hive-count"
if ((WITH_HIVE_COUNT == 1)); then
  hive_extra=""
fi
p2_run_logged "p0_hive" "Hive" "$P2_RUN_DIR/p0_hive.log" \
  "Hive is not ready for realtime table validation." \
  "cd $P2_PROJECT_ROOT && bin/p0_cluster_health_check.sh --module hive $hive_extra" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_cluster_health_check.sh --module hive $hive_extra"

p2_finish

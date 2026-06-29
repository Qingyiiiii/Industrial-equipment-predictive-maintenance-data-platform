#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
CHECK_ONLY=0
DO_TRINO=1
DO_DORIS=0
ALLOW_SWAPOFF=0
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/start_extended_query_mode.sh [--check-only] [--trino] [--doris] [--trino-only] [--doris-only] [--allow-swapoff] [--hosts "hadoop1 hadoop2 hadoop3"]

Defaults:
  Start/check Trino only. Doris is heavier and starts only with --doris or --doris-only.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check-only)
      CHECK_ONLY=1
      shift
      ;;
    --trino)
      DO_TRINO=1
      shift
      ;;
    --doris)
      DO_DORIS=1
      shift
      ;;
    --trino-only)
      DO_TRINO=1
      DO_DORIS=0
      shift
      ;;
    --doris-only)
      DO_TRINO=0
      DO_DORIS=1
      shift
      ;;
    --allow-swapoff)
      ALLOW_SWAPOFF=1
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

p2_init "start_extended_query_mode" "${ORIGINAL_ARGS[@]}"
p2_header "P2 extended query mode startup"
printf 'check_only=%s\ntrino=%s\ndoris=%s\nallow_swapoff=%s\nhosts=%s\n\n' \
  "$CHECK_ONLY" "$DO_TRINO" "$DO_DORIS" "$ALLOW_SWAPOFF" "${HOSTS[*]}"

start_trino() {
  local host
  if ((DO_TRINO == 0)); then
    p2_report SKIP "start_trino" "not selected"
    return 0
  fi
  if ((CHECK_ONLY == 1)); then
    for host in "${HOSTS[@]}"; do
      local status
      status="$(p2_run_on "$host" "export JAVA_HOME=/export/server/jdk25; /export/server/trino/bin/launcher status 2>/dev/null || true" 2>&1 || true)"
      if printf '%s\n' "$status" | grep -Eq '^(INFO: )?Running|^Running as'; then
        p2_report PASS "$host:Trino" "$(printf '%s' "$status" | head -n 1)"
      else
        p2_report SKIP "$host:Trino" "${status:-not running}"
      fi
      p2_check_port "$host" 8080 "Trino http"
    done
    return 0
  fi
  trino_start_command() {
    cat <<'REMOTE_CMD'
export JAVA_HOME=/export/server/jdk25
export PATH=$JAVA_HOME/bin:$PATH
status="$(/export/server/trino/bin/launcher status 2>/dev/null || true)"
if printf '%s\n' "$status" | grep -Eq '^(INFO: )?Running|^Running as'; then
  echo 'Trino already running'
else
  /export/server/trino/bin/launcher start
fi
for i in $(seq 1 30); do
  if /export/server/trino/bin/launcher status 2>/dev/null | grep -Eq '^(INFO: )?Running|^Running as' \
    && ss -lntp 2>/dev/null | grep -qE '[:.]8080[[:space:]]'; then
    /export/server/trino/bin/launcher status || true
    exit 0
  fi
  sleep 2
done
/export/server/trino/bin/launcher status || true
tail -n 120 /export/data/trino/var/log/launcher.log 2>/dev/null || true
tail -n 160 /export/data/trino/var/log/server.log 2>/dev/null || true
exit 1
REMOTE_CMD
  }
  for host in "${HOSTS[@]}"; do
    local log="$P2_RUN_DIR/start_trino_${host}.log"
    local cmd
    cmd="$(trino_start_command)"
    p2_run_logged "start_trino_${host}" "Trino" "$log" \
      "Trino failed to start on $host." \
      "ssh common@$host 'export JAVA_HOME=/export/server/jdk25; /export/server/trino/bin/launcher status; tail -n 120 /export/data/trino/var/log/server.log'" \
      p2_run_on "$host" "$cmd"
  done
}

check_or_disable_swap() {
  local host="$1"
  local log="$P2_RUN_DIR/doris_swap_${host}.log"
  local cmd
  if ((ALLOW_SWAPOFF == 1)); then
    cmd="sudo swapoff -a; swapon --show || true"
  else
    cmd="if swapon --show | awk 'NR>1 {found=1} END {exit found?0:1}'; then echo 'swap_is_on'; exit 3; else echo 'swap_is_off'; fi"
  fi
  if p2_run_on "$host" "$cmd" > "$log" 2>&1; then
    p2_report PASS "doris_swap_${host}" "log=$log"
  else
    p2_report FAIL "doris_swap_${host}" "swap is on or check failed; log=$log"
    p2_failure_template "doris_swap_${host}" "Doris" 3 "$log" "" \
      "Doris BE requires swap off. Use --allow-swapoff only if you accept sudo swapoff -a." \
      "ssh common@$host 'swapon --show; sudo swapoff -a'"
    return 1
  fi
}

start_doris() {
  local host
  if ((DO_DORIS == 0)); then
    p2_report SKIP "start_doris" "not selected"
    return 0
  fi
  for host in "${HOSTS[@]}"; do
    check_or_disable_swap "$host" || return 1
  done
  if ((CHECK_ONLY == 1)); then
    p2_check_port hadoop1 18030 "Doris FE http"
    p2_check_port hadoop1 9030 "Doris FE mysql"
    for host in "${HOSTS[@]}"; do
      p2_check_port "$host" 18040 "Doris BE web"
      p2_check_port "$host" 9050 "Doris BE heartbeat"
      p2_check_port "$host" 9060 "Doris BE brpc"
      p2_check_port "$host" 8060 "Doris BE http"
    done
    return 0
  fi

  p2_run_logged "start_doris_fe" "Doris" "$P2_RUN_DIR/start_doris_fe.log" \
    "Doris FE failed to start; inspect FE logs and port ownership." \
    "ss -lntp | egrep '18030|9030'; tail -n 120 /export/server/doris/fe/log/fe.out" \
    bash -lc "if ss -lntp 2>/dev/null | grep -qE '[:.](18030|9030)[[:space:]]'; then echo 'Doris FE already has listener'; else /export/server/doris/fe/bin/start_fe.sh --daemon; fi"

  for host in "${HOSTS[@]}"; do
    local log="$P2_RUN_DIR/start_doris_be_${host}.log"
    local cmd="if ps -ef | egrep 'doris_be|palo_be' | grep -v grep >/dev/null 2>&1; then echo 'Doris BE already running'; else mkdir -p /export/data/doris/be; chown -R common:common /export/data/doris/be 2>/dev/null || true; /export/server/doris/be/bin/start_be.sh --daemon; fi"
    p2_run_logged "start_doris_be_${host}" "Doris" "$log" \
      "Doris BE failed to start on $host." \
      "ssh common@$host 'ps -ef | egrep \"doris_be|palo_be\" | grep -v grep; ss -lntp | egrep \"18040|9050|9060|8060\"; tail -n 120 /export/server/doris/be/log/be.out'" \
      p2_run_on "$host" "$cmd"
  done
}

start_trino
start_doris

p2_run_logged "p0_trino_doris" "Trino/Doris" "$P2_RUN_DIR/p0_trino_doris.log" \
  "Extended query mode health check reported warnings or failures." \
  "cd $P2_PROJECT_ROOT && bin/p0_cluster_health_check.sh --module trino-doris" \
  bash -lc "cd '$P2_PROJECT_ROOT' && bin/p0_cluster_health_check.sh --module trino-doris"

p2_finish

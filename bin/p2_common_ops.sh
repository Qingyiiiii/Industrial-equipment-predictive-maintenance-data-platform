#!/usr/bin/env bash
set -uo pipefail

P2_PASS_COUNT=${P2_PASS_COUNT:-0}
P2_WARN_COUNT=${P2_WARN_COUNT:-0}
P2_SKIP_COUNT=${P2_SKIP_COUNT:-0}
P2_FAIL_COUNT=${P2_FAIL_COUNT:-0}

p2_now_id() {
  date '+%Y%m%d_%H%M%S'
}

p2_init() {
  local name="${1:-p2}"
  shift || true
  P2_BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  P2_PROJECT_ROOT="${P2_PROJECT_ROOT:-$(cd "$P2_BIN_DIR/.." && pwd)}"
  P2_RUN_ID="${P2_RUN_ID:-$(p2_now_id)}"
  P2_VALIDATION_ROOT="${P2_VALIDATION_ROOT:-$P2_PROJECT_ROOT/data/metropt_quality/validation_runs}"
  P2_RUN_DIR="${P2_RUN_DIR:-$P2_VALIDATION_ROOT/${name}_${P2_RUN_ID}}"
  P2_SUMMARY="${P2_SUMMARY:-$P2_RUN_DIR/summary.tsv}"
  P2_COMMAND_LOG="${P2_COMMAND_LOG:-$P2_RUN_DIR/command.tsv}"
  mkdir -p "$P2_RUN_DIR"
  if [[ ! -f "$P2_SUMMARY" ]]; then
    printf 'level\tstep\tmessage\n' > "$P2_SUMMARY"
  fi
  if [[ ! -f "$P2_COMMAND_LOG" ]]; then
    printf 'key\tvalue\n' > "$P2_COMMAND_LOG"
    printf 'run_id\t%s\n' "$P2_RUN_ID" >> "$P2_COMMAND_LOG"
    printf 'script\t%s\n' "${0:-unknown}" >> "$P2_COMMAND_LOG"
    printf 'start_time\t%s\n' "$(date '+%F %T')" >> "$P2_COMMAND_LOG"
    printf 'project_root\t%s\n' "$P2_PROJECT_ROOT" >> "$P2_COMMAND_LOG"
    printf 'run_dir\t%s\n' "$P2_RUN_DIR" >> "$P2_COMMAND_LOG"
    printf 'command\t' >> "$P2_COMMAND_LOG"
    printf '%q ' "$0" "$@" >> "$P2_COMMAND_LOG"
    printf '\n' >> "$P2_COMMAND_LOG"
  fi
}

p2_header() {
  local title="$1"
  printf '%s\n' "$title"
  printf 'run_id=%s\n' "$P2_RUN_ID"
  printf 'project_root=%s\n' "$P2_PROJECT_ROOT"
  printf 'run_dir=%s\n\n' "$P2_RUN_DIR"
}

p2_report() {
  local level="$1"
  local step="$2"
  local message="$3"
  case "$level" in
    PASS) ((P2_PASS_COUNT++));;
    WARN) ((P2_WARN_COUNT++));;
    SKIP) ((P2_SKIP_COUNT++));;
    FAIL) ((P2_FAIL_COUNT++));;
  esac
  printf '[%s] %s - %s\n' "$level" "$step" "$message"
  if [[ -n "${P2_SUMMARY:-}" ]]; then
    printf '%s\t%s\t%s\n' "$level" "$step" "$message" >> "$P2_SUMMARY"
  fi
}

p2_extract_application_id() {
  local log_path="$1"
  if [[ -f "$log_path" ]]; then
    grep -Eo 'application_[0-9]+_[0-9]+' "$log_path" | tail -n 1 || true
  fi
}

p2_failure_template() {
  local failed_step="$1"
  local component="$2"
  local return_code="$3"
  local primary_log="$4"
  local application_id="${5:-}"
  local diagnosis_hint="${6:-}"
  local next_command="${7:-}"

  cat <<EOF

[FAILURE_DETAIL]
failed_step=$failed_step
component=$component
return_code=$return_code
primary_log=$primary_log
application_id=${application_id:-N/A}
diagnosis_hint=${diagnosis_hint:-N/A}
next_command=${next_command:-N/A}
[/FAILURE_DETAIL]

EOF
}

p2_local_short() {
  hostname -s 2>/dev/null || hostname
}

p2_local_fqdn() {
  hostname -f 2>/dev/null || hostname
}

p2_is_local_host() {
  local host="$1"
  local short fqdn
  short="$(p2_local_short)"
  fqdn="$(p2_local_fqdn)"
  [[ "$host" == "$short" || "$host" == "$fqdn" || "$host" == "localhost" ]]
}

p2_run_on() {
  local host="$1"
  local cmd="$2"
  if p2_is_local_host "$host"; then
    bash -lc "$cmd"
  else
    ssh -n -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
      "$host" "bash -lc $(printf '%q' "$cmd")"
  fi
}

p2_run_logged() {
  local step="$1"
  local component="$2"
  local log_path="$3"
  local diagnosis_hint="$4"
  local next_command="$5"
  shift 5

  mkdir -p "$(dirname "$log_path")"
  {
    printf 'step=%s\n' "$step"
    printf 'component=%s\n' "$component"
    printf 'start=%s\n' "$(date '+%F %T')"
    printf 'command='
    printf '%q ' "$@"
    printf '\n\n'
  } > "$log_path"

  "$@" >> "$log_path" 2>&1
  local rc=$?
  {
    printf '\nend=%s\n' "$(date '+%F %T')"
    printf 'return_code=%s\n' "$rc"
  } >> "$log_path"

  if ((rc == 0)); then
    p2_report PASS "$step" "rc=0 log=$log_path"
  else
    local app_id
    app_id="$(p2_extract_application_id "$log_path")"
    p2_report FAIL "$step" "rc=$rc log=$log_path"
    p2_failure_template "$step" "$component" "$rc" "$log_path" "$app_id" "$diagnosis_hint" "$next_command"
  fi
  return "$rc"
}

p2_check_jps() {
  local host="$1"
  local regex="$2"
  local desc="$3"
  local required="${4:-optional}"
  local out
  out="$(p2_run_on "$host" "jps -l 2>/dev/null | grep -E '$regex' || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    p2_report PASS "$host:$desc" "$(printf '%s' "$out" | head -n 1)"
    return 0
  fi
  if [[ "$required" == "required" ]]; then
    p2_report FAIL "$host:$desc" "process missing"
  else
    p2_report SKIP "$host:$desc" "process not running"
  fi
  return 1
}

p2_check_port() {
  local host="$1"
  local port="$2"
  local desc="$3"
  local required="${4:-optional}"
  local out
  out="$(p2_run_on "$host" "ss -lntp 2>/dev/null | grep -E '[:.]$port[[:space:]]' || true" 2>&1 || true)"
  if [[ -n "$out" ]]; then
    p2_report PASS "$host:$desc" "port $port listening"
    return 0
  fi
  if [[ "$required" == "required" ]]; then
    p2_report FAIL "$host:$desc" "port $port not listening"
  else
    p2_report SKIP "$host:$desc" "port $port not listening"
  fi
  return 1
}

p2_append_command_result() {
  local rc="$1"
  printf 'end_time\t%s\n' "$(date '+%F %T')" >> "$P2_COMMAND_LOG"
  printf 'return_code\t%s\n' "$rc" >> "$P2_COMMAND_LOG"
  printf 'pass\t%s\nwarn\t%s\nskip\t%s\nfail\t%s\n' \
    "$P2_PASS_COUNT" "$P2_WARN_COUNT" "$P2_SKIP_COUNT" "$P2_FAIL_COUNT" >> "$P2_COMMAND_LOG"
}

p2_finish() {
  local rc=0
  printf '\nSUMMARY pass=%d warn=%d skip=%d fail=%d\n' \
    "$P2_PASS_COUNT" "$P2_WARN_COUNT" "$P2_SKIP_COUNT" "$P2_FAIL_COUNT"
  if ((P2_FAIL_COUNT > 0)); then
    rc=1
  fi
  p2_append_command_result "$rc"
  exit "$rc"
}

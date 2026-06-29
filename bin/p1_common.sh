#!/usr/bin/env bash
# Common helpers for P1 acceptance and maintenance scripts.

P1_PASS_COUNT=${P1_PASS_COUNT:-0}
P1_WARN_COUNT=${P1_WARN_COUNT:-0}
P1_FAIL_COUNT=${P1_FAIL_COUNT:-0}
P1_EXTENSION_SKIPPED=${P1_EXTENSION_SKIPPED:-0}

p1_now_id() {
  date '+%Y%m%d_%H%M%S'
}

p1_init() {
  local name="${1:-p1}"
  P1_BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  P1_PROJECT_ROOT="${P1_PROJECT_ROOT:-$(cd "$P1_BIN_DIR/.." && pwd)}"
  P1_RUN_ID="${P1_RUN_ID:-$(p1_now_id)}"
  P1_LOG_ROOT="${P1_LOG_ROOT:-$P1_PROJECT_ROOT/data/metropt_quality/p1_logs}"
  P1_LOG_DIR="${P1_LOG_DIR:-$P1_LOG_ROOT/${name}_${P1_RUN_ID}}"
  mkdir -p "$P1_LOG_DIR"
}

p1_header() {
  local title="$1"
  printf '%s\n' "$title"
  printf 'run_id=%s\n' "$P1_RUN_ID"
  printf 'project_root=%s\n' "$P1_PROJECT_ROOT"
  printf 'log_dir=%s\n\n' "$P1_LOG_DIR"
}

p1_report() {
  local level="$1"
  local step="$2"
  local message="$3"
  case "$level" in
    PASS) ((P1_PASS_COUNT++));;
    WARN) ((P1_WARN_COUNT++));;
    FAIL) ((P1_FAIL_COUNT++));;
  esac
  printf '[%s] %s - %s\n' "$level" "$step" "$message"
}

p1_extract_application_id() {
  local log_path="$1"
  if [[ -f "$log_path" ]]; then
    grep -Eo 'application_[0-9]+_[0-9]+' "$log_path" | tail -n 1 || true
  fi
}

p1_failure_template() {
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

p1_run_logged() {
  local step="$1"
  local component="$2"
  local log_path="$3"
  local diagnosis_hint="$4"
  local next_command="$5"
  shift 5

  mkdir -p "$(dirname "$log_path")"
  printf '[START] %s -> %s\n' "$step" "$log_path"
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
    p1_report PASS "$step" "rc=0"
  else
    local app_id
    app_id="$(p1_extract_application_id "$log_path")"
    p1_report FAIL "$step" "rc=$rc log=$log_path"
    p1_failure_template "$step" "$component" "$rc" "$log_path" "$app_id" "$diagnosis_hint" "$next_command"
  fi
  return "$rc"
}

p1_run_p0_gate() {
  local step="$1"
  local command="$2"
  local log_path="$P1_LOG_DIR/${step}.log"
  p1_run_logged "$step" "P0" "$log_path" \
    "P0 gate failed; fix base service or config drift before running P1." \
    "cd $P1_PROJECT_ROOT && $command" \
    bash -lc "cd '$P1_PROJECT_ROOT' && $command"
}

p1_safe_tail() {
  local path="$1"
  local lines="${2:-80}"
  if [[ -f "$path" ]]; then
    tail -n "$lines" "$path"
  else
    printf 'missing log: %s\n' "$path"
  fi
}

p1_finish() {
  local extension_code="${1:-0}"
  printf '\nSUMMARY pass=%d warn=%d fail=%d\n' "$P1_PASS_COUNT" "$P1_WARN_COUNT" "$P1_FAIL_COUNT"
  if ((P1_FAIL_COUNT > 0)); then
    exit 1
  fi
  if ((extension_code == 1 || P1_EXTENSION_SKIPPED == 1)); then
    exit 4
  fi
  exit 0
}

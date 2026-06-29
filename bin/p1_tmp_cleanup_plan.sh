#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p1_common.sh"

TMP_ROOT="/home/common/tmp"
KEEP_DAYS=7
APPLY=0

usage() {
  cat <<'USAGE'
Usage:
  bin/p1_tmp_cleanup_plan.sh [--dry-run|--apply] [--days N] [--tmp-root PATH]

Default behavior is --dry-run. The script never scans /export/server or HDFS.
Protected paths:
  /home/common/tmp/pycharm_Design
  /home/common/tmp/metropt_quality/MetroPT3_AirCompressor.csv
  /home/common/tmp/config_backups
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      APPLY=0
      shift
      ;;
    --apply)
      APPLY=1
      shift
      ;;
    --days)
      KEEP_DAYS="${2:-}"
      shift 2
      ;;
    --tmp-root)
      TMP_ROOT="${2:-}"
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

if [[ "$KEEP_DAYS" =~ [^0-9] ]]; then
  echo "--days must be a non-negative integer" >&2
  exit 2
fi

p1_init "tmp_cleanup"
p1_header "P1 tmp cleanup plan"

MANIFEST_DIR="$TMP_ROOT/cleanup_manifests"
MANIFEST="$MANIFEST_DIR/${P1_RUN_ID}_cleanup_manifest.tsv"
mkdir -p "$MANIFEST_DIR"
printf 'action\treason\tbytes\tmtime\tpath\n' > "$MANIFEST"

is_protected_path() {
  local path="$1"
  case "$path" in
    "$TMP_ROOT") return 0;;
    "$TMP_ROOT/pycharm_Design"|"$TMP_ROOT/pycharm_Design"/*) return 0;;
    "$TMP_ROOT/metropt_quality/MetroPT3_AirCompressor.csv") return 0;;
    "$TMP_ROOT/config_backups"|"$TMP_ROOT/config_backups"/*) return 0;;
    "$MANIFEST_DIR"|"$MANIFEST_DIR"/*) return 0;;
  esac
  return 1
}

candidate_seen_file="$P1_LOG_DIR/candidates.seen"
: > "$candidate_seen_file"

add_candidate() {
  local path="$1"
  local reason="$2"
  [[ -e "$path" ]] || return 0
  is_protected_path "$path" && return 0
  [[ "$path" == "$TMP_ROOT/"* ]] || return 0
  grep -Fxq "$path" "$candidate_seen_file" && return 0
  printf '%s\n' "$path" >> "$candidate_seen_file"

  local bytes mtime action
  bytes="$(du -sb "$path" 2>/dev/null | awk '{print $1}')"
  mtime="$(stat -c '%y' "$path" 2>/dev/null | cut -d'.' -f1)"
  action="DRY_RUN"
  if ((APPLY == 1)); then
    action="DELETE"
    rm -rf -- "$path"
  fi
  printf '%s\t%s\t%s\t%s\t%s\n' "$action" "$reason" "${bytes:-0}" "${mtime:-unknown}" "$path" >> "$MANIFEST"
}

# Hive temp conf can be rebuilt by metropt_hive_mr_count_check.sh.
add_candidate "$TMP_ROOT/hive-conf-jdk8" "rebuildable_hive_temp_conf"
add_candidate "$TMP_ROOT/hive-mr-jdk8-conf" "rebuildable_hive_temp_conf"

# Old SQL/HQL/tmp files outside protected project tree.
while IFS= read -r path; do
  add_candidate "$path" "old_sql_or_temp_file"
done < <(find "$TMP_ROOT" -xdev -type f \( -name '*.sql' -o -name '*.hql' -o -name '*.tmp' -o -name '*~' \) -mtime +"$KEEP_DAYS" 2>/dev/null)

# Old logs outside current project and backup directories.
while IFS= read -r path; do
  add_candidate "$path" "old_log_file"
done < <(find "$TMP_ROOT" -xdev -type f \( -name '*.log' -o -name '*.out' \) -mtime +"$KEEP_DAYS" 2>/dev/null)

# Top-level residual directories that are neither current project, dataset, backups, nor manifests.
while IFS= read -r path; do
  case "$path" in
    "$TMP_ROOT/pycharm_Design"|"$TMP_ROOT/metropt_quality"|"$TMP_ROOT/config_backups"|"$TMP_ROOT/cleanup_manifests") ;;
    *)
      add_candidate "$path" "top_level_tmp_residual"
      ;;
  esac
done < <(find "$TMP_ROOT" -xdev -mindepth 1 -maxdepth 1 -type d -mtime +"$KEEP_DAYS" 2>/dev/null)

candidate_count="$(awk 'NR>1 {c++} END {print c+0}' "$MANIFEST")"
if ((candidate_count == 0)); then
  p1_report PASS "tmp_cleanup_plan" "no cleanup candidates"
else
  if ((APPLY == 1)); then
    p1_report PASS "tmp_cleanup_apply" "deleted $candidate_count candidates; manifest=$MANIFEST"
  else
    p1_report PASS "tmp_cleanup_dry_run" "listed $candidate_count candidates; manifest=$MANIFEST"
  fi
fi

printf '\nmanifest=%s\n' "$MANIFEST"
column -t -s $'\t' "$MANIFEST" 2>/dev/null || cat "$MANIFEST"

p1_finish

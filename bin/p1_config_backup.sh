#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p1_common.sh"

HOSTS=(${P1_HOSTS:-hadoop1 hadoop2 hadoop3})
BACKUP_BASE="/home/common/tmp/config_backups"

CONF_DIRS=(
  "/export/server/hadoop/etc/hadoop"
  "/export/server/hive/conf"
  "/export/server/spark/conf"
  "/export/server/flink/conf"
  "/export/server/kafka/config"
  "/export/server/trino/etc"
  "/export/server/doris/fe/conf"
  "/export/server/doris/be/conf"
)

usage() {
  cat <<'USAGE'
Usage:
  bin/p1_config_backup.sh [--hosts "hadoop1 hadoop2 hadoop3"] [--backup-base PATH]

Creates:
  /home/common/tmp/config_backups/YYYYMMDD_HHMMSS/<host>/...
  /home/common/tmp/config_backups/YYYYMMDD_HHMMSS/<host>_manifest.tsv
  /home/common/tmp/config_backups/YYYYMMDD_HHMMSS/<host>.tar.gz
  /home/common/tmp/config_backups/YYYYMMDD_HHMMSS/p0_config_drift_check.log
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts)
      read -r -a HOSTS <<< "${2:-}"
      shift 2
      ;;
    --backup-base)
      BACKUP_BASE="${2:-}"
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

p1_init "config_backup"
p1_header "P1 config backup"

BACKUP_ROOT="$BACKUP_BASE/$P1_RUN_ID"
mkdir -p "$BACKUP_ROOT"
SUMMARY_MANIFEST="$BACKUP_ROOT/manifest_all.tsv"
printf 'host\tsha256\tbytes\tmtime\tpath\n' > "$SUMMARY_MANIFEST"

LOCAL_SHORT="$(hostname -s 2>/dev/null || hostname)"
LOCAL_FQDN="$(hostname -f 2>/dev/null || hostname)"

is_local_host() {
  [[ "$1" == "$LOCAL_SHORT" || "$1" == "$LOCAL_FQDN" || "$1" == "localhost" ]]
}

remote_exists_dirs_command() {
  local quoted=""
  local dir
  for dir in "${CONF_DIRS[@]}"; do
    quoted+=" $(printf '%q' "$dir")"
  done
  cat <<CMD
for d in$quoted; do
  if [[ -d "\$d" ]]; then
    printf '%s\\n' "\$d"
  fi
done
CMD
}

copy_host_configs() {
  local host="$1"
  local host_dir="$BACKUP_ROOT/$host"
  local dir_list="$P1_LOG_DIR/${host}_dirs.txt"
  mkdir -p "$host_dir"

  if is_local_host "$host"; then
    bash -lc "$(remote_exists_dirs_command)" > "$dir_list"
  else
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
      "$host" "bash -lc $(printf '%q' "$(remote_exists_dirs_command)")" > "$dir_list"
  fi

  if [[ ! -s "$dir_list" ]]; then
    p1_report WARN "config_backup_$host" "no configured conf dirs found"
    return 0
  fi

  local tar_args=""
  while IFS= read -r dir; do
    tar_args+=" $(printf '%q' "${dir#/}")"
  done < "$dir_list"

  if is_local_host "$host"; then
    bash -lc "tar -C / -czf - $tar_args" | tar -xzf - -C "$host_dir"
  else
    ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR \
      "$host" "bash -lc $(printf '%q' "tar -C / -czf - $tar_args")" | tar -xzf - -C "$host_dir"
  fi

  local manifest="$BACKUP_ROOT/${host}_manifest.tsv"
  printf 'host\tsha256\tbytes\tmtime\tpath\n' > "$manifest"
  while IFS= read -r -d '' file; do
    local sha bytes mtime rel
    sha="$(sha256sum "$file" | awk '{print $1}')"
    bytes="$(stat -c '%s' "$file")"
    mtime="$(stat -c '%y' "$file" | cut -d'.' -f1)"
    rel="${file#"$host_dir/"}"
    printf '%s\t%s\t%s\t%s\t/%s\n' "$host" "$sha" "$bytes" "$mtime" "$rel" >> "$manifest"
    printf '%s\t%s\t%s\t%s\t/%s\n' "$host" "$sha" "$bytes" "$mtime" "$rel" >> "$SUMMARY_MANIFEST"
  done < <(find "$host_dir" -type f -print0)

  tar -C "$BACKUP_ROOT" -czf "$BACKUP_ROOT/${host}.tar.gz" "$host" "${host}_manifest.tsv"
  p1_report PASS "config_backup_$host" "backup=$BACKUP_ROOT/${host}.tar.gz"
}

for host in "${HOSTS[@]}"; do
  copy_host_configs "$host"
done

p0_log="$BACKUP_ROOT/p0_config_drift_check.log"
if p1_run_logged "p0_config_drift_after_backup" "P0" "$p0_log" \
  "P0 config drift check failed after backup." \
  "cd $P1_PROJECT_ROOT && bin/p0_config_drift_check.sh" \
  bash -lc "cd '$P1_PROJECT_ROOT' && bin/p0_config_drift_check.sh"; then
  p1_report PASS "config_backup_p0_snapshot" "p0_config_drift_check.log captured"
fi

printf '\nbackup_root=%s\nsummary_manifest=%s\n' "$BACKUP_ROOT" "$SUMMARY_MANIFEST"
p1_finish

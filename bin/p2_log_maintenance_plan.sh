#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/p2_common_ops.sh"

HOSTS=(${P2_HOSTS:-hadoop1 hadoop2 hadoop3})
APPLY=0
KEEP_DAYS=7
ORIGINAL_ARGS=("$@")

usage() {
  cat <<'USAGE'
Usage:
  bin/p2_log_maintenance_plan.sh [--dry-run|--apply] [--keep-days N] [--hosts "hadoop1 hadoop2 hadoop3"]

Default:
  --dry-run. It writes candidate manifests only and deletes nothing.
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
    --keep-days)
      KEEP_DAYS="${2:-}"
      shift 2
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

if [[ "$KEEP_DAYS" =~ [^0-9] || -z "$KEEP_DAYS" ]]; then
  echo "--keep-days must be a non-negative integer" >&2
  exit 2
fi

p2_init "log_maintenance" "${ORIGINAL_ARGS[@]}"
p2_header "P2 log maintenance plan"
printf 'apply=%s\nkeep_days=%s\nhosts=%s\n\n' "$APPLY" "$KEEP_DAYS" "${HOSTS[*]}"

scan_host_logs() {
  local host="$1"
  local log="$P2_RUN_DIR/${host}_log_scan.log"
  local manifest="$P2_RUN_DIR/${host}_delete_candidates.tsv"
  local cmd
  cmd=$(cat <<'CMD'
set +e
KEEP_DAYS="__KEEP_DAYS__"
APPLY="__APPLY__"
dirs=(
  /export/logs/hive
  /export/server/hadoop/logs
  /export/server/hadoop/logs/userlogs
  /export/server/flink/log
  /export/logs/kafka
  /export/server/doris/fe/log
  /export/server/doris/be/log
  /export/data/trino/var/log
  /export/server/trino/var/log
)

for d in "${dirs[@]}"; do
  echo "===== $d ====="
  if [[ ! -d "$d" ]]; then
    echo "status=missing"
    continue
  fi
  du -sh "$d" 2>/dev/null || true
  echo "largest_files="
  find "$d" -type f -printf '%s\t%TY-%Tm-%Td %TH:%TM\t%p\n' 2>/dev/null | sort -nr | head -n 10 || true
  echo "oldest_files="
  find "$d" -type f -printf '%T@\t%TY-%Tm-%Td %TH:%TM\t%p\n' 2>/dev/null | sort -n | head -n 5 | cut -f2- || true
  echo "newest_files="
  find "$d" -type f -printf '%T@\t%TY-%Tm-%Td %TH:%TM\t%p\n' 2>/dev/null | sort -nr | head -n 5 | cut -f2- || true
  if command -v lsof >/dev/null 2>&1; then
    active_count=$(lsof +D "$d" 2>/dev/null | awk 'NR>1 {c++} END {print c+0}')
    echo "active_open_files=$active_count"
  else
    echo "active_open_files=unknown_lsof_missing"
  fi
  while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    case "$(basename "$f")" in
      hive-metastore.out|hiveserver2.out|kafka-server.out|server.log|fe.out|fe.log|be.out|be.INFO|be.WARNING|be.ERROR|*.pid|*.lock)
        continue
        ;;
    esac
    size=$(stat -c '%s' "$f" 2>/dev/null || echo 0)
    mtime=$(stat -c '%y' "$f" 2>/dev/null | cut -d'.' -f1 || echo unknown)
    printf 'CANDIDATE\t%s\t%s\t%s\tolder_than_%s_days_rotated_log\n' "$f" "$size" "$mtime" "$KEEP_DAYS"
    if [[ "$APPLY" == "1" && -f "$f" ]]; then
      rm -f -- "$f"
    fi
  done < <(find "$d" -type f -mtime +"$KEEP_DAYS" \( -name '*.gz' -o -name '*.log.*' -o -name '*.out.*' -o -name '*.INFO.*' -o -name '*.WARNING.*' -o -name '*.ERROR.*' -o -name '*.[0-9]*' \) 2>/dev/null)
done

if [[ "$APPLY" == "1" ]]; then
  echo "apply=done"
else
  echo "apply=dry-run"
fi
CMD
)
  cmd="${cmd//__KEEP_DAYS__/$KEEP_DAYS}"
  cmd="${cmd//__APPLY__/$APPLY}"
  if p2_run_on "$host" "$cmd" > "$log" 2>&1; then
    printf 'path\tbytes\tmtime\treason\n' > "$manifest"
    awk -F'\t' '$1=="CANDIDATE"{print $2 "\t" $3 "\t" $4 "\t" $5}' "$log" >> "$manifest"
    if [[ -s "$manifest" && "$(wc -l < "$manifest")" -gt 1 ]]; then
      p2_report WARN "${host}_log_candidates" "candidates=$(($(wc -l < "$manifest") - 1)) manifest=$manifest"
    else
      p2_report PASS "${host}_log_candidates" "no delete candidates; scan=$log"
    fi
  else
    p2_report FAIL "${host}_log_scan" "log scan failed; log=$log"
  fi
}

for host in "${HOSTS[@]}"; do
  scan_host_logs "$host"
done

draft="$P2_RUN_DIR/logrotate_draft.conf"
cat > "$draft" <<'EOF'
# Draft only. Review before installing under /etc/logrotate.d.
/export/logs/hive/*.log /export/logs/hive/*.out /export/logs/kafka/*.out /export/server/flink/log/*.log {
    daily
    rotate 7
    compress
    missingok
    copytruncate
}
EOF
p2_report PASS "logrotate_draft" "draft=$draft"

p2_finish

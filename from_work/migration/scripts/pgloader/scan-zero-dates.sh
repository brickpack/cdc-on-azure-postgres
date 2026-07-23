#!/usr/bin/env bash
# Scan one or more MySQL databases for zero-date values ('0000-00-00') in
# DATE/DATETIME/TIMESTAMP columns. MySQL-only — no PostgreSQL comparison.
#
# Usage:
#   bash scan-zero-dates.sh --mysql-host HOST --mysql-user USER --mysql-db-list dbs.txt
#   bash scan-zero-dates.sh --env-file ./migration.env --mysql-db-list dbs.txt
#
# --mysql-db-list FILE is a plain text file, one MySQL database name per line.
# Blank lines and lines starting with # are ignored.
#
# Options:
#   --mysql-host HOST        MySQL FQDN (or env MYSQL_FQDN)
#   --mysql-user USER        MySQL user (or env MYSQL_USER)
#   --mysql-db DB            Single MySQL database (or env MYSQL_DB) — use instead of --mysql-db-list
#   --mysql-db-list FILE     Text file listing MySQL databases to scan, one per line
#   --mysql-pass-file FILE   Read MySQL password from FILE (or env MYSQL_PASSWORD)
#   --env-file FILE          Source variables from FILE
#
# Optional env:
#   LOG_DIR=~/migration/logs
#
# Output:
#   Console table per database, plus a combined CSV report in LOG_DIR.

set -euo pipefail

# --- Helpers ---
read_password_file() {
  local f="$1"
  [[ -f "$f" && -r "$f" ]] || { echo "Cannot read password file: $f" >&2; return 1; }
  head -1 "$f" | tr -d '\n'
}

csv_field() {
  local v="${1//\"/\"\"}"
  printf '"%s"' "$v"
}

# --- Argument parsing ---
ENV_FILE=""
CLI_MYSQL_HOST="" CLI_MYSQL_USER="" CLI_MYSQL_DB="" CLI_MYSQL_PASS_FILE=""
CLI_MYSQL_DB_LIST=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysql-host)       CLI_MYSQL_HOST="$2"; shift 2 ;;
    --mysql-user)       CLI_MYSQL_USER="$2"; shift 2 ;;
    --mysql-db)         CLI_MYSQL_DB="$2"; shift 2 ;;
    --mysql-db-list)    CLI_MYSQL_DB_LIST="$2"; shift 2 ;;
    --mysql-pass-file)  CLI_MYSQL_PASS_FILE="$2"; shift 2 ;;
    --env-file)         ENV_FILE="$2"; shift 2 ;;
    -h|--help)          sed -n '2,/^[^#]/{ /^#/s/^# \?//p }' "$0"; exit 0 ;;
    *)                  echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# --- Load variables: env-file → environment → flags (flags win) ---
if [[ -n "$ENV_FILE" ]]; then
  # shellcheck source=/dev/null
  source "$ENV_FILE"
fi

[[ -n "$CLI_MYSQL_HOST" ]] && MYSQL_FQDN="$CLI_MYSQL_HOST"
[[ -n "$CLI_MYSQL_USER" ]] && MYSQL_USER="$CLI_MYSQL_USER"
[[ -n "$CLI_MYSQL_DB" ]]   && MYSQL_DB="$CLI_MYSQL_DB"
MYSQL_DB_LIST="${CLI_MYSQL_DB_LIST:-${MYSQL_DB_LIST:-}}"

[[ -n "$CLI_MYSQL_PASS_FILE" ]] && MYSQL_PASSWORD="$(read_password_file "$CLI_MYSQL_PASS_FILE")"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  for _f in "${HOME:+${HOME}/.mysql_migration_pw}" /etc/secrets/mysql_pw; do
    [[ -n "$_f" && -f "$_f" && -r "$_f" ]] && { MYSQL_PASSWORD="$(read_password_file "$_f")"; break; }
  done
fi

if [[ -z "$MYSQL_DB_LIST" && -z "${MYSQL_DB:-}" ]]; then
  echo "Must specify --mysql-db-list FILE or --mysql-db DB (or MYSQL_DB_LIST/MYSQL_DB env)." >&2
  exit 1
fi
if [[ -n "$MYSQL_DB_LIST" && ! -r "$MYSQL_DB_LIST" ]]; then
  echo "Cannot read --mysql-db-list file: $MYSQL_DB_LIST" >&2
  exit 1
fi

_missing=()
[[ -z "${MYSQL_FQDN:-}" ]]     && _missing+=(MYSQL_FQDN)
[[ -z "${MYSQL_USER:-}" ]]     && _missing+=(MYSQL_USER)
[[ -z "${MYSQL_PASSWORD:-}" ]] && _missing+=(MYSQL_PASSWORD)
if [[ ${#_missing[@]} -gt 0 ]]; then
  echo "Missing required variables: ${_missing[*]}" >&2
  exit 1
fi

export MYSQL_PWD="$MYSQL_PASSWORD"

LOG_DIR="${LOG_DIR:-${HOME}/migration/logs}"
TS=$(date -u +"%Y%m%d_%H%M%S")
mkdir -p "$LOG_DIR"
REPORT="${LOG_DIR}/scan-zero-dates_${TS}.txt"
CSV="${LOG_DIR}/scan-zero-dates_${TS}.csv"

# --- Databases to scan ---
DATABASES=()
if [[ -n "$MYSQL_DB_LIST" ]]; then
  while IFS= read -r _line || [[ -n "$_line" ]]; do
    _line="${_line%%#*}"
    _line="$(echo "$_line" | xargs || true)"
    [[ -n "$_line" ]] && DATABASES+=("$_line")
  done < "$MYSQL_DB_LIST"
else
  DATABASES+=("$MYSQL_DB")
fi

if [[ ${#DATABASES[@]} -eq 0 ]]; then
  echo "No databases found to scan (empty --mysql-db-list?)." >&2
  exit 1
fi

MYCNF="$(mktemp /tmp/.my_zerodate_XXXXXX.cnf)"
chmod 600 "$MYCNF"
cat >"$MYCNF" <<MYCNF
[client]
host=${MYSQL_FQDN}
user=${MYSQL_USER}
password=${MYSQL_PASSWORD}
ssl-mode=REQUIRED
MYCNF
trap 'rm -f "$MYCNF"' EXIT

my_q() { mysql --defaults-file="$MYCNF" -D "$CUR_DB" -N -e "$1" 2>/dev/null; }
mysql_ident() { local t="${1//\`/\`\`}"; printf '`%s`' "$t"; }
sql_literal()  { printf '%s' "${1//\'/\'\'}"; }

# Use a FIFO for tee so no pipe-buffer data is lost (process substitution is unreliable).
TMPD="$(mktemp -d /tmp/scan_zd_XXXXXX)"
trap 'rm -rf "$TMPD"; rm -f "$MYCNF"' EXIT
_FIFO="$TMPD/tee_fifo"
mkfifo "$_FIFO"
tee "$REPORT" < "$_FIFO" &
TEE_PID=$!
exec > "$_FIFO" 2>&1

echo "================================================================"
echo " MySQL zero-date scan — $(date -u)"
echo " MySQL: ${MYSQL_FQDN}"
echo " Databases (${#DATABASES[@]}): ${DATABASES[*]}"
echo " Report file: ${REPORT}"
echo " CSV file:    ${CSV}"
echo "================================================================"

echo "database,table,column,data_type,zero_count,mysql_nulls" > "$CSV"

TOTAL_FOUND=0

for CUR_DB in "${DATABASES[@]}"; do
  echo ""
  echo "--- Database: ${CUR_DB} ---"

  if ! my_q "SELECT 1" >/dev/null 2>&1; then
    echo "  >>> ERROR: could not connect to database '${CUR_DB}' — skipping."
    continue
  fi

  printf '\n%-40s %-24s %-12s %10s %10s\n' \
    "TABLE" "COLUMN" "TYPE" "MY_ZEROS" "MY_NULLS"
  printf '%s\n' "$(printf -- '-%.0s' {1..100})"

  _db_found=0

  all_date_cols=$(my_q "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA='$(sql_literal "${CUR_DB}")'
      AND DATA_TYPE IN ('date','datetime','timestamp')
    ORDER BY TABLE_NAME, ORDINAL_POSITION" || true)

  if [[ -z "$all_date_cols" ]]; then
    echo "  (none -- no date/datetime/timestamp columns found)"
    continue
  fi

  while IFS=$'\t' read -r tbl col dtype; do
    [[ -z "$tbl" || -z "$col" ]] && continue

    ti=$(mysql_ident "$tbl")
    mi=$(mysql_ident "$col")

    has_zeros=$(my_q "SELECT 1 FROM ${ti} WHERE ${mi} IS NOT NULL AND CAST(${mi} AS CHAR) LIKE '0000-00-00%' LIMIT 1" || true)
    [[ -z "$has_zeros" ]] && continue

    counts=$(my_q "SELECT
        SUM(CASE WHEN ${mi} IS NOT NULL AND CAST(${mi} AS CHAR) LIKE '0000-00-00%' THEN 1 ELSE 0 END),
        SUM(CASE WHEN ${mi} IS NULL THEN 1 ELSE 0 END)
      FROM ${ti}" || echo "ERR")

    [[ "$counts" == "ERR" || -z "$counts" ]] && continue

    zero_count=$(printf '%s' "$counts" | cut -f1)
    mysql_nulls=$(printf '%s' "$counts" | cut -f2)
    [[ "$zero_count" == "NULL" || -z "$zero_count" ]] && zero_count=0
    [[ "$mysql_nulls" == "NULL" || -z "$mysql_nulls" ]] && mysql_nulls=0
    if [[ ! "$zero_count" =~ ^[0-9]+$ ]]; then continue; fi
    [[ "$zero_count" -eq 0 ]] && continue

    _db_found=$(( _db_found + 1 ))
    TOTAL_FOUND=$(( TOTAL_FOUND + 1 ))

    printf '%-40s %-24s %-12s %10s %10s\n' \
      "$tbl" "$col" "$dtype" "$zero_count" "$mysql_nulls"

    printf '%s,%s,%s,%s,%s,%s\n' \
      "$(csv_field "$CUR_DB")" "$(csv_field "$tbl")" "$(csv_field "$col")" \
      "$(csv_field "$dtype")" "$zero_count" "$mysql_nulls" >> "$CSV"

  done <<< "$all_date_cols"

  if [[ "$_db_found" -eq 0 ]]; then
    echo "  (none -- no zero-date values found)"
  fi
done

echo ""
echo "================================================================"
echo "Summary: ${TOTAL_FOUND} column(s) with zero-date values across ${#DATABASES[@]} database(s)."
echo "================================================================"
echo ""
echo "Full report: ${REPORT}"
echo "CSV report:  ${CSV}"

exec 1>&- 2>&-
wait "$TEE_PID" 2>/dev/null || true

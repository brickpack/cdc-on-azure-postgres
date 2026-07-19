#!/usr/bin/env bash
# Pre-flight inventory: MySQL objects that pgloader will NOT migrate automatically.
# Reports views, stored procedures, functions, triggers, FULLTEXT indexes, and events.
# Each item must be assessed and, if needed, recreated manually on PostgreSQL.
#
# Exits 0 if no unmigrated objects found; exits 1 if any are detected.
#
# Usage:
#   bash 1-mysql-objects-inventory.sh --env-file ./migration.env
#   bash 1-mysql-objects-inventory.sh \
#     --mysql-host mydb.mysql.database.azure.com \
#     --mysql-user migrate_admin \
#     --mysql-db the_db \
#     --mysql-pass-file /etc/secrets/mysql_pw
#   bash 1-mysql-objects-inventory.sh --env-file ./migration.env --show-ddl
#
# Options:
#   --mysql-host HOST        MySQL FQDN (or env MYSQL_FQDN)
#   --mysql-user USER        MySQL user (or env MYSQL_USER)
#   --mysql-db DB            MySQL database (or env MYSQL_DB)
#   --mysql-pass-file FILE   Read MySQL password from FILE (or env MYSQL_PASSWORD)
#   --env-file FILE          Source variables from FILE
#   --show-ddl               Include full CREATE DDL for each object in the report
#
# Optional env:
#   LOG_DIR=~/migration/logs
#
# Requires on MySQL: SELECT on information_schema; SHOW CREATE VIEW / PROCEDURE /
#   FUNCTION / TRIGGER / EVENT privileges for DDL output.

set -euo pipefail

# --- Helpers ---
read_password_file() {
  local f="$1"
  [[ -f "$f" && -r "$f" ]] || { echo "Cannot read password file: $f" >&2; return 1; }
  head -1 "$f" | tr -d '\n'
}

# --- Argument parsing ---
ENV_FILE=""
SHOW_DDL=0
CLI_MYSQL_HOST="" CLI_MYSQL_USER="" CLI_MYSQL_DB="" CLI_MYSQL_PASS_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysql-host)       CLI_MYSQL_HOST="$2"; shift 2 ;;
    --mysql-user)       CLI_MYSQL_USER="$2"; shift 2 ;;
    --mysql-db)         CLI_MYSQL_DB="$2"; shift 2 ;;
    --mysql-pass-file)  CLI_MYSQL_PASS_FILE="$2"; shift 2 ;;
    --env-file)         ENV_FILE="$2"; shift 2 ;;
    --show-ddl)         SHOW_DDL=1; shift ;;
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

[[ -n "$CLI_MYSQL_PASS_FILE" ]] && MYSQL_PASSWORD="$(read_password_file "$CLI_MYSQL_PASS_FILE")"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  for _f in "${HOME:+${HOME}/.mysql_migration_pw}" /etc/secrets/mysql_pw; do
    [[ -n "$_f" && -f "$_f" && -r "$_f" ]] && { MYSQL_PASSWORD="$(read_password_file "$_f")"; break; }
  done
fi

_missing=()
[[ -z "${MYSQL_FQDN:-}" ]]     && _missing+=(MYSQL_FQDN)
[[ -z "${MYSQL_USER:-}" ]]     && _missing+=(MYSQL_USER)
[[ -z "${MYSQL_DB:-}" ]]       && _missing+=(MYSQL_DB)
[[ -z "${MYSQL_PASSWORD:-}" ]] && _missing+=(MYSQL_PASSWORD)
if [[ ${#_missing[@]} -gt 0 ]]; then
  echo "Missing required variables: ${_missing[*]}" >&2
  exit 1
fi

export MYSQL_PWD="$MYSQL_PASSWORD"

LOG_DIR="${LOG_DIR:-${HOME}/migration/logs}"
TS=$(date -u +"%Y%m%d_%H%M%S")
mkdir -p "$LOG_DIR"
REPORT="${LOG_DIR}/mysql-objects-inventory_${TS}.txt"

# --- MySQL connection setup ---
MYCNF="$(mktemp /tmp/.my_inventory_XXXXXX.cnf)"
chmod 600 "$MYCNF"
cat >"$MYCNF" <<MYCNF
[client]
host=${MYSQL_FQDN}
user=${MYSQL_USER}
password=${MYSQL_PASSWORD}
database=${MYSQL_DB}
ssl-mode=REQUIRED
MYCNF
trap 'rm -f "$MYCNF"' EXIT

# --- Query helpers ---
my_q()    { mysql --defaults-file="$MYCNF" -D "$MYSQL_DB" -N -B -e "$1" 2>/dev/null; }
my_show() { mysql --defaults-file="$MYCNF" -D "$MYSQL_DB" -e "$1" 2>/dev/null; }

exec > >(tee "$REPORT")
exec 2>&1

echo "================================================================"
echo " MySQL pre-flight object inventory — $(date -u)"
echo " MySQL: ${MYSQL_FQDN} / ${MYSQL_DB}"
echo " Report: ${REPORT}"
echo " DDL output: $([[ $SHOW_DDL -eq 1 ]] && echo 'enabled (--show-ddl)' || echo 'disabled (re-run with --show-ddl)')"
echo "================================================================"
echo ""

# --- Pre-flight: verify connection ---
echo "==> Checking MySQL connection..."
if ! my_q "SELECT 1" >/dev/null; then
  echo "MySQL connection failed: ${MYSQL_USER}@${MYSQL_FQDN}/${MYSQL_DB}" >&2
  exit 1
fi
echo "    OK"
echo ""

EXIT=0
CNT_VIEWS=0
CNT_PROCS=0
CNT_FUNCS=0
CNT_TRIGGERS=0
CNT_FULLTEXT=0
CNT_EVENTS=0

# ── 1. Views ──────────────────────────────────────────────────────────────────
echo "--- Views ---"
mapfile -t _VIEWS < <(my_q "SELECT TABLE_NAME
    FROM information_schema.VIEWS
    WHERE TABLE_SCHEMA = '${MYSQL_DB}'
    ORDER BY TABLE_NAME" || true)

if [[ ${#_VIEWS[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_VIEWS=${#_VIEWS[@]}
  EXIT=1
  for _v in "${_VIEWS[@]}"; do
    [[ -z "$_v" ]] && continue
    echo "  VIEW: ${_v}"
    if [[ $SHOW_DDL -eq 1 ]]; then
      echo "  --- DDL ---"
      my_show "SHOW CREATE VIEW \`${MYSQL_DB}\`.\`${_v}\`\G" | sed 's/^/  /'
      echo ""
    fi
  done
fi
echo ""

# ── 2. Stored Procedures ──────────────────────────────────────────────────────
echo "--- Stored Procedures ---"
mapfile -t _PROCS < <(my_q "SELECT ROUTINE_NAME
    FROM information_schema.ROUTINES
    WHERE ROUTINE_SCHEMA = '${MYSQL_DB}' AND ROUTINE_TYPE = 'PROCEDURE'
    ORDER BY ROUTINE_NAME" || true)

if [[ ${#_PROCS[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_PROCS=${#_PROCS[@]}
  EXIT=1
  for _p in "${_PROCS[@]}"; do
    [[ -z "$_p" ]] && continue
    echo "  PROCEDURE: ${_p}"
    if [[ $SHOW_DDL -eq 1 ]]; then
      echo "  --- DDL ---"
      my_show "SHOW CREATE PROCEDURE \`${MYSQL_DB}\`.\`${_p}\`\G" | sed 's/^/  /'
      echo ""
    fi
  done
fi
echo ""

# ── 3. Functions ──────────────────────────────────────────────────────────────
echo "--- Functions ---"
mapfile -t _FUNCS < <(my_q "SELECT ROUTINE_NAME
    FROM information_schema.ROUTINES
    WHERE ROUTINE_SCHEMA = '${MYSQL_DB}' AND ROUTINE_TYPE = 'FUNCTION'
    ORDER BY ROUTINE_NAME" || true)

if [[ ${#_FUNCS[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_FUNCS=${#_FUNCS[@]}
  EXIT=1
  for _f in "${_FUNCS[@]}"; do
    [[ -z "$_f" ]] && continue
    echo "  FUNCTION: ${_f}"
    if [[ $SHOW_DDL -eq 1 ]]; then
      echo "  --- DDL ---"
      my_show "SHOW CREATE FUNCTION \`${MYSQL_DB}\`.\`${_f}\`\G" | sed 's/^/  /'
      echo ""
    fi
  done
fi
echo ""

# ── 4. Triggers ───────────────────────────────────────────────────────────────
echo "--- Triggers ---"
mapfile -t _TRIGGERS < <(my_q "SELECT CONCAT(TRIGGER_NAME,'|',ACTION_TIMING,'|',EVENT_MANIPULATION,'|',EVENT_OBJECT_TABLE)
    FROM information_schema.TRIGGERS
    WHERE TRIGGER_SCHEMA = '${MYSQL_DB}'
    ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME" || true)

if [[ ${#_TRIGGERS[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_TRIGGERS=${#_TRIGGERS[@]}
  EXIT=1
  for _t in "${_TRIGGERS[@]}"; do
    [[ -z "$_t" ]] && continue
    IFS='|' read -r _tname _timing _event _table <<< "$_t"
    echo "  TRIGGER: ${_tname}  (${_timing} ${_event} ON ${_table})"
    if [[ $SHOW_DDL -eq 1 ]]; then
      echo "  --- DDL ---"
      my_show "SHOW CREATE TRIGGER \`${_tname}\`\G" | sed 's/^/  /'
      echo ""
    fi
  done
fi
echo ""

# ── 5. FULLTEXT Indexes ───────────────────────────────────────────────────────
echo "--- FULLTEXT Indexes ---"
mapfile -t _FULLTEXT < <(my_q "SELECT CONCAT(TABLE_NAME,'|',INDEX_NAME,'|',GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ','))
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = '${MYSQL_DB}' AND INDEX_TYPE = 'FULLTEXT'
    GROUP BY TABLE_NAME, INDEX_NAME
    ORDER BY TABLE_NAME, INDEX_NAME" || true)

if [[ ${#_FULLTEXT[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_FULLTEXT=${#_FULLTEXT[@]}
  EXIT=1
  echo "  NOTE: pgloader silently skips FULLTEXT indexes. Recreate as PostgreSQL"
  echo "  GIN indexes with tsvector columns if full-text search is required."
  echo ""
  for _idx in "${_FULLTEXT[@]}"; do
    [[ -z "$_idx" ]] && continue
    IFS='|' read -r _tbl _idxname _cols <<< "$_idx"
    echo "  FULLTEXT INDEX: ${_idxname}  ON ${_tbl} (${_cols})"
  done
fi
echo ""

# ── 6. Events (scheduled jobs) ────────────────────────────────────────────────
echo "--- Events (scheduled jobs) ---"
mapfile -t _EVENTS < <(my_q "SELECT CONCAT(EVENT_NAME,'|',EVENT_TYPE,'|',IFNULL(INTERVAL_VALUE,''),'|',IFNULL(INTERVAL_FIELD,''),'|',STATUS)
    FROM information_schema.EVENTS
    WHERE EVENT_SCHEMA = '${MYSQL_DB}'
    ORDER BY EVENT_NAME" || true)

if [[ ${#_EVENTS[@]} -eq 0 ]]; then
  echo "(none)"
else
  CNT_EVENTS=${#_EVENTS[@]}
  EXIT=1
  for _ev in "${_EVENTS[@]}"; do
    [[ -z "$_ev" ]] && continue
    IFS='|' read -r _evname _evtype _ivval _ivfield _evstatus <<< "$_ev"
    _interval=""
    [[ -n "$_ivval" ]] && _interval=" every ${_ivval} ${_ivfield}"
    echo "  EVENT: ${_evname}  (${_evtype}${_interval} / ${_evstatus})"
    if [[ $SHOW_DDL -eq 1 ]]; then
      echo "  --- DDL ---"
      my_show "SHOW CREATE EVENT \`${_evname}\`\G" | sed 's/^/  /'
      echo ""
    fi
  done
fi
echo ""

# ── Summary ────────────────────────────────────────────────────────────────────
echo "================================================================"
_total=$(( CNT_VIEWS + CNT_PROCS + CNT_FUNCS + CNT_TRIGGERS + CNT_FULLTEXT + CNT_EVENTS ))
if [[ $EXIT -eq 0 ]]; then
  echo "Summary: No unmigrated objects found — pgloader will handle everything."
else
  echo "Summary: ${_total} object(s) found that pgloader will NOT migrate:"
  echo ""
  printf '  %-24s %d\n' "Views:"              "$CNT_VIEWS"
  printf '  %-24s %d\n' "Stored Procedures:"  "$CNT_PROCS"
  printf '  %-24s %d\n' "Functions:"          "$CNT_FUNCS"
  printf '  %-24s %d\n' "Triggers:"           "$CNT_TRIGGERS"
  printf '  %-24s %d\n' "FULLTEXT Indexes:"   "$CNT_FULLTEXT"
  printf '  %-24s %d\n' "Events:"             "$CNT_EVENTS"
  echo ""
  echo "  Each item above must be assessed and manually recreated on PostgreSQL"
  echo "  before routing application traffic to the new database."
  if [[ $SHOW_DDL -eq 0 ]]; then
    echo ""
    echo "  Re-run with --show-ddl to include full CREATE statements in the report."
  fi
fi
echo "================================================================"

exit "$EXIT"

#!/usr/bin/env bash
# MySQL → PostgreSQL full load using native pgloader.
# Uses "WITH include drop" — drops and recreates target tables each run.
#
# Passwords: set via env (MYSQL_PASSWORD / PG_PASSWORD), --*-pass-file, or a secrets env-file
#   containing MYSQL_PASSWORD=... and PG_PASSWORD=... lines.
# Use the write-passwords.sh helper to create password files or env-files without secrets in shell history.
#
# Usage:
# Full migration (passwords from files):
# bash 2-run-pgloader.sh \
#   --mysql-host mydb.mysql.database.azure.com \
#   --mysql-user migrate_admin \
#   --mysql-db the_db \
#   --mysql-pass-file /etc/secrets/mysql_pw \
#   --pg-host mypg.postgres.database.azure.com \
#   --pg-user migrate_admin \
#   --pg-db the_db \
#   --pg-pass-file /etc/secrets/pg_pw

# # Dry-run first (just checks connectivity and lists tables):
# bash 2-run-pgloader.sh --dry-run \
#   --mysql-host mydb.mysql.database.azure.com \
#   --mysql-user migrate_admin \
#   --mysql-db the_db \
#   --mysql-pass-file /etc/secrets/mysql_pw \
#   --pg-host mypg.postgres.database.azure.com \
#   --pg-user migrate_admin \
#   --pg-db the_db \
#   --pg-pass-file /etc/secrets/pg_pw

# # Or with an env-file:
# bash 2-run-pgloader.sh --env-file ./my-migration.env --dry-run

#
# Options:
#   --dry-run                Verify connectivity; do not migrate.
#   --mysql-host HOST        MySQL FQDN (or env MYSQL_FQDN)
#   --mysql-user USER        MySQL user (or env MYSQL_USER)
#   --mysql-db DB            MySQL database (or env MYSQL_DB)
#   --mysql-pass-file FILE   Read MySQL password from FILE (or env MYSQL_PASSWORD)
#   --pg-host HOST           PostgreSQL FQDN (or env PG_FQDN)
#   --pg-user USER           PostgreSQL user (or env PG_USER)
#   --pg-db DB               PostgreSQL database (or env PG_DB)
#   --pg-pass-file FILE      Read PG password from FILE (or env PG_PASSWORD)
#   --schema SCHEMA          Target PostgreSQL schema (default: public; or env PG_SCHEMA)
#   --exclude-tables LIST    Comma-separated table names to skip (added to EXCLUDING TABLE NAMES MATCHING)
#   --env-file FILE          Source variables from FILE
#


set -euo pipefail

# --- Helpers ---
# Read a prompt from the terminal if available (az ssh has no /dev/tty; Docker
# on macOS can drain stdin, so prefer /dev/tty when it exists).
_prompt_read() {
  local prompt="$1" varname="$2"
  # /dev/tty exists as a device file even without a controlling terminal, so
  # [[ -r /dev/tty ]] is not sufficient — probe whether it can actually be opened.
  if ( exec </dev/tty ) 2>/dev/null; then
    read -r -p "$prompt" "$varname" </dev/tty
  else
    read -r -p "$prompt" "$varname"
  fi
}

urlenc() {
  python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$1"
}

read_password_file() {
  local f="$1"
  [[ -f "$f" && -r "$f" ]] || { echo "Cannot read password file: $f" >&2; return 1; }
  head -1 "$f" | tr -d '\n'
}

# --- Argument parsing ---
DRY_RUN=0
ENV_FILE=""
CLI_MYSQL_HOST="" CLI_MYSQL_USER="" CLI_MYSQL_DB="" CLI_MYSQL_PASS_FILE=""
CLI_PG_HOST="" CLI_PG_USER="" CLI_PG_DB="" CLI_PG_PASS_FILE=""
CLI_EXCLUDE_TABLES=""
CLI_SCHEMA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)          DRY_RUN=1; shift ;;
    --mysql-host)       CLI_MYSQL_HOST="$2"; shift 2 ;;
    --mysql-user)       CLI_MYSQL_USER="$2"; shift 2 ;;
    --mysql-db)         CLI_MYSQL_DB="$2"; shift 2 ;;
    --mysql-pass-file)  CLI_MYSQL_PASS_FILE="$2"; shift 2 ;;
    --pg-host)          CLI_PG_HOST="$2"; shift 2 ;;
    --pg-user)          CLI_PG_USER="$2"; shift 2 ;;
    --pg-db)            CLI_PG_DB="$2"; shift 2 ;;
    --pg-pass-file)     CLI_PG_PASS_FILE="$2"; shift 2 ;;
    --schema)           CLI_SCHEMA="$2"; shift 2 ;;
    --exclude-tables)   CLI_EXCLUDE_TABLES="$2"; shift 2 ;;
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
[[ -n "$CLI_PG_HOST" ]]    && PG_FQDN="$CLI_PG_HOST"
[[ -n "$CLI_PG_USER" ]]    && PG_USER="$CLI_PG_USER"
[[ -n "$CLI_PG_DB" ]]      && PG_DB="$CLI_PG_DB"
[[ -n "$CLI_SCHEMA" ]]     && PG_SCHEMA="$CLI_SCHEMA"
PG_SCHEMA="${PG_SCHEMA:-public}"

# Passwords: flag file → env → well-known files from write-passwords.sh.
[[ -n "$CLI_MYSQL_PASS_FILE" ]] && MYSQL_PASSWORD="$(read_password_file "$CLI_MYSQL_PASS_FILE")"
[[ -n "$CLI_PG_PASS_FILE" ]]    && PG_PASSWORD="$(read_password_file "$CLI_PG_PASS_FILE")"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
  for _f in "${HOME:+${HOME}/.mysql_migration_pw}" /etc/secrets/mysql_pw; do
    [[ -n "$_f" && -f "$_f" && -r "$_f" ]] && { MYSQL_PASSWORD="$(read_password_file "$_f")"; break; }
  done
fi
if [[ -z "${PG_PASSWORD:-}" ]]; then
  for _f in "${HOME:+${HOME}/.pg_migration_pw}" /etc/secrets/pg_pw; do
    [[ -n "$_f" && -f "$_f" && -r "$_f" ]] && { PG_PASSWORD="$(read_password_file "$_f")"; break; }
  done
fi

_missing=()
[[ -z "${MYSQL_FQDN:-}" ]]     && _missing+=(MYSQL_FQDN)
[[ -z "${MYSQL_USER:-}" ]]     && _missing+=(MYSQL_USER)
[[ -z "${MYSQL_DB:-}" ]]       && _missing+=(MYSQL_DB)
[[ -z "${MYSQL_PASSWORD:-}" ]] && _missing+=(MYSQL_PASSWORD)
[[ -z "${PG_FQDN:-}" ]]       && _missing+=(PG_FQDN)
[[ -z "${PG_USER:-}" ]]       && _missing+=(PG_USER)
[[ -z "${PG_DB:-}" ]]         && _missing+=(PG_DB)
[[ -z "${PG_PASSWORD:-}" ]]   && _missing+=(PG_PASSWORD)
if [[ ${#_missing[@]} -gt 0 ]]; then
  echo "Missing required variables: ${_missing[*]}" >&2
  exit 1
fi

export PGPASSWORD="$PG_PASSWORD"
export PGHOST="$PG_FQDN"
export PGSSLMODE=require
export MYSQL_PWD="$MYSQL_PASSWORD"

# --- Pre-flight: verify both connections ---
echo "==> Checking PostgreSQL connection..."
if ! psql -U "$PG_USER" -d "$PG_DB" --no-password -c 'SELECT 1' >/dev/null; then
  echo "PostgreSQL connection failed: ${PG_USER}@${PG_FQDN}/${PG_DB}" >&2
  exit 1
fi

echo "==> Checking MySQL connection..."
if ! mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -e 'SELECT 1' "$MYSQL_DB" >/dev/null; then
  echo "MySQL connection failed: ${MYSQL_USER}@${MYSQL_FQDN}/${MYSQL_DB}" >&2
  exit 1
fi

# --- Dry-run: report what we found and exit ---
if [[ $DRY_RUN -eq 1 ]]; then
  echo "=== DRY RUN — $(date -u) ==="
  echo ""
  echo "pgloader: $(pgloader --version 2>&1 | head -1)"
  echo ""
  echo "MySQL ${MYSQL_USER}@${MYSQL_FQDN}/${MYSQL_DB}:"
  echo "  Version: $(mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -e 'SELECT version()' "$MYSQL_DB" 2>/dev/null)"
  echo "  Tables:"
  mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -e "
    SELECT table_name AS \`Table\`,
           table_rows AS \`Est. Rows\`,
           ROUND(data_length/1024/1024, 2) AS \`Data MB\`
    FROM information_schema.tables
    WHERE table_schema = '${MYSQL_DB}' AND table_type = 'BASE TABLE'
    ORDER BY table_rows DESC;" "$MYSQL_DB" 2>/dev/null || echo "  (could not list tables)"
  echo ""
  echo "PostgreSQL ${PG_USER}@${PG_FQDN}/${PG_DB}:"
  echo "  Version: $(psql -U "$PG_USER" -d "$PG_DB" --no-password -At -c 'SELECT version()' 2>/dev/null)"
  echo "  Existing tables in '${PG_SCHEMA}':"
  psql -U "$PG_USER" -d "$PG_DB" --no-password -At -c \
    "SELECT tablename FROM pg_tables WHERE schemaname='${PG_SCHEMA}' ORDER BY tablename;" 2>/dev/null \
    | sed 's/^/    /' || echo "    (none)"
  echo ""
  echo "=== Pre-flight OK ==="
  exit 0
fi

# --- Actual migration ---
_job_start=$(date +%s)

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not found in PATH. Install docker.io first." >&2
  exit 1
fi

if ! docker image inspect dimitri/pgloader:latest >/dev/null 2>&1; then
  echo "Pulling pgloader image..."
  docker pull dimitri/pgloader:latest
fi

LOG_DIR="${LOG_DIR:-${HOME}/migration/logs}"
TS=$(date -u +"%Y%m%d_%H%M%S")_$$
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/pgloader-migrate_${TS}.log"

MU=$(urlenc "$MYSQL_USER")
MP=$(urlenc "$MYSQL_PASSWORD")
PU=$(urlenc "$PG_USER")

WORKERS="${PGLOADER_WORKERS:-4}"
CONCURRENCY="${PGLOADER_CONCURRENCY:-1}"
NET_READ_TIMEOUT="${PGLOADER_NET_READ_TIMEOUT:-3600}"
NET_WRITE_TIMEOUT="${PGLOADER_NET_WRITE_TIMEOUT:-3600}"

MYSQL_URI="mysql://${MU}:${MP}@${MYSQL_FQDN}:3306/${MYSQL_DB}?useSSL=true"
PG_URI="postgresql://${PU}@${PG_FQDN}:5432/${PG_DB}?sslmode=require"

# Build extra EXCLUDING clause from --exclude-tables
EXCLUDE_TABLES_CLAUSE=""
if [[ -n "$CLI_EXCLUDE_TABLES" ]]; then
  IFS=',' read -ra _tables <<< "$CLI_EXCLUDE_TABLES"
  for _t in "${_tables[@]}"; do
    _t="$(echo "$_t" | xargs)"  # trim whitespace
    EXCLUDE_TABLES_CLAUSE="${EXCLUDE_TABLES_CLAUSE}, '${_t}'"
  done
fi

# AUTO_INCREMENT cols with extra EXTRA qualifiers (e.g. INVISIBLE) don't match pgloader's
# 'with extra auto_increment' type rule. Detect them and add explicit column rules first.
mapfile -t _ai_extra_cols < <(
  mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -B \
    -e "SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME, '|', DATA_TYPE)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '${MYSQL_DB}'
          AND EXTRA LIKE '%auto_increment%'
          AND EXTRA != 'auto_increment'
        ORDER BY TABLE_NAME, COLUMN_NAME" "$MYSQL_DB" 2>/dev/null || true
)

# Columns with charset-introducer defaults (e.g. _utf8mb3'value') produce invalid PG DDL.
mapfile -t _ci_cols < <(
  mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -B \
    -e "SELECT CONCAT(TABLE_NAME,'.', COLUMN_NAME)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '${MYSQL_DB}'
          AND COLUMN_DEFAULT LIKE '\\_utf8%'
        ORDER BY TABLE_NAME, COLUMN_NAME" "$MYSQL_DB" 2>/dev/null || true
)

# Columns with zero-date defaults (e.g. '0000-00-00', '0000-00-00 00:00:00') are valid in
# MySQL but rejected by PostgreSQL at CREATE TABLE time. Detect and add 'drop default' rules.
# Must include the PG type — pgloader produces NIL type if 'column x drop default' has no 'to <type>'.
mapfile -t _zd_cols < <(
  mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -B \
    -e "SELECT CONCAT(TABLE_NAME,'.', COLUMN_NAME,'|',DATA_TYPE)
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '${MYSQL_DB}'
          AND (COLUMN_DEFAULT = '0000-00-00'
            OR COLUMN_DEFAULT = '0000-00-00 00:00:00'
            OR COLUMN_DEFAULT LIKE '0000-00-00%')
          AND DATA_TYPE NOT IN ('binary','varbinary','tinyblob','blob','mediumblob','longblob')
        ORDER BY TABLE_NAME, COLUMN_NAME" "$MYSQL_DB" 2>/dev/null || true
)



# Column rules must come before type rules — pgloader is first-match.
CAST_RULES=()

if [[ ${#_ai_extra_cols[@]} -gt 0 ]]; then
  echo "==> ${#_ai_extra_cols[@]} AUTO_INCREMENT+qualifier column(s) need explicit CAST rules:"
  for _col_info in "${_ai_extra_cols[@]}"; do
    [[ -n "$_col_info" ]] || continue
    IFS='|' read -r _col_ref _data_type <<< "$_col_info"
    _pg_type="$([[ "$_data_type" == "bigint" ]] && echo bigserial || echo serial)"
    echo "    column ${_col_ref} → ${_pg_type}"
    CAST_RULES+=("column ${_col_ref} to ${_pg_type}")
  done
fi

if [[ ${#_ci_cols[@]} -gt 0 ]]; then
  echo "==> ${#_ci_cols[@]} column(s) with charset-introducer defaults: dropping those defaults"
  for _ci in "${_ci_cols[@]}"; do
    [[ -n "$_ci" ]] || continue
    echo "    column ${_ci} → text drop default"
    CAST_RULES+=("column ${_ci} to text drop default")
  done
fi

if [[ ${#_zd_cols[@]} -gt 0 ]]; then
  echo "==> ${#_zd_cols[@]} column(s) with zero-date defaults: dropping those defaults"
  for _zd_info in "${_zd_cols[@]}"; do
    [[ -n "$_zd_info" ]] || continue
    IFS='|' read -r _zd _zd_dtype <<< "$_zd_info"
    _zd_pgtype=$(case "$_zd_dtype" in
      date)      echo "date drop typemod" ;;
      datetime)  echo "timestamp drop typemod" ;;
      timestamp) echo "timestamptz drop typemod" ;;
      *)         echo "text drop typemod" ;;
    esac)
    echo "    column ${_zd} → ${_zd_pgtype} drop default"
    CAST_RULES+=("column ${_zd} to ${_zd_pgtype} drop default")
  done
fi

# NOT NULL columns that contain actual NULLs violate PG constraints during COPY.
# MySQL allows this inconsistency when strict mode was not enforced historically.
#
# Scan strategy: fetch estimated row counts alongside column metadata (single query),
# then split tables into two buckets:
#   - Large tables (estimated rows > PGLOADER_NULL_SCAN_THRESHOLD, default 500k):
#     skip data scan; add 'drop not null' proactively for all their NOT NULL columns.
#     This is safe (more permissive, never less), avoids multi-minute full-table scans.
#   - Small/medium tables: run one MAX(c IS NULL),... query per table (fast).
#
# Conflict detection: if a column already has an explicit CAST rule (e.g. from the
# charset-introducer path above), append 'drop not null' rather than adding a duplicate.
declare -A _existing_col_rule_idx
for _ri in "${!CAST_RULES[@]}"; do
  if [[ "${CAST_RULES[$_ri]}" =~ ^column[[:space:]]+([^[:space:]]+) ]]; then
    _existing_col_rule_idx["${BASH_REMATCH[1]}"]=$_ri
  fi
done

_null_scan_threshold="${PGLOADER_NULL_SCAN_THRESHOLD:-500000}"
echo "==> Scanning for NOT NULL columns with actual NULL data (null-scan-threshold=${_null_scan_threshold} rows)..."

mapfile -t _nn_col_list < <(
  mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -B \
    -e "SELECT CONCAT(c.TABLE_NAME,'.',c.COLUMN_NAME,'|',c.DATA_TYPE,'|',c.COLUMN_TYPE,'|',IFNULL(t.TABLE_ROWS,0))
        FROM information_schema.COLUMNS c
        JOIN information_schema.TABLES t
          ON t.TABLE_SCHEMA=c.TABLE_SCHEMA AND t.TABLE_NAME=c.TABLE_NAME
        WHERE c.TABLE_SCHEMA='${MYSQL_DB}'
          AND c.IS_NULLABLE='NO'
          AND c.COLUMN_KEY!='PRI'
          AND c.EXTRA NOT LIKE '%auto_increment%'
          AND t.TABLE_TYPE='BASE TABLE'
          AND c.DATA_TYPE NOT IN ('binary','varbinary','tinyblob','blob','mediumblob','longblob')
        ORDER BY t.TABLE_ROWS, c.TABLE_NAME, c.COLUMN_NAME" "$MYSQL_DB" 2>/dev/null || true
)

# Group columns by table; track estimated row count per table.
declare -A _nn_tbl_names_buf _nn_tbl_dtypes_buf _nn_tbl_ctypes_buf _nn_tbl_rows
_nn_sep=$'\x01'
for _col_info in "${_nn_col_list[@]}"; do
  [[ -n "$_col_info" ]] || continue
  IFS='|' read -r _col_ref _data_type _column_type _tbl_rows <<< "$_col_info"
  IFS='.' read -r _nn_tbl _nn_col <<< "$_col_ref"
  _nn_tbl_names_buf["$_nn_tbl"]+="${_nn_col}${_nn_sep}"
  _nn_tbl_dtypes_buf["$_nn_tbl"]+="${_data_type}${_nn_sep}"
  _nn_tbl_ctypes_buf["$_nn_tbl"]+="${_column_type}${_nn_sep}"
  _nn_tbl_rows["$_nn_tbl"]="${_tbl_rows}"
done

_nn_violation_count=0
_nn_skipped_tables=()
for _nn_tbl in "${!_nn_tbl_names_buf[@]}"; do
  IFS="$_nn_sep" read -ra _col_names  <<< "${_nn_tbl_names_buf[$_nn_tbl]}"
  IFS="$_nn_sep" read -ra _col_dtypes <<< "${_nn_tbl_dtypes_buf[$_nn_tbl]}"
  IFS="$_nn_sep" read -ra _col_ctypes <<< "${_nn_tbl_ctypes_buf[$_nn_tbl]}"
  _est_rows="${_nn_tbl_rows[$_nn_tbl]:-0}"

  # Helper: emit a cast rule (or amend existing) for a column.
  # 'drop not null' must come before any 'using ...' clause in pgloader's CAST grammar.
  _emit_nn_rule() {
    local col_ref="$1" pg_type="$2" reason="$3"
    local pg_type_nn
    if [[ "$pg_type" == *" using "* ]]; then
      pg_type_nn="${pg_type/ using / drop not null using }"
    else
      pg_type_nn="${pg_type} drop not null"
    fi
    if [[ -n "${_existing_col_rule_idx[$col_ref]+x}" ]]; then
      local ri="${_existing_col_rule_idx[$col_ref]}"
      if [[ "${CAST_RULES[$ri]}" == *" using "* ]]; then
        CAST_RULES[$ri]="${CAST_RULES[$ri]/ using / drop not null using }"
      else
        CAST_RULES[$ri]="${CAST_RULES[$ri]} drop not null"
      fi
      echo "    column ${col_ref} → appended drop not null to existing rule (${reason})"
    else
      echo "    column ${col_ref} → ${pg_type_nn} (${reason})"
      CAST_RULES+=("column ${col_ref} to ${pg_type_nn}")
    fi
    _nn_violation_count=$(( _nn_violation_count + 1 ))
  }

  if (( _est_rows > _null_scan_threshold )); then
    # Large table: skip data scan, add drop not null proactively for all NOT NULL cols.
    _nn_skipped_tables+=("${_nn_tbl}(~${_est_rows})")
    for _i in "${!_col_names[@]}"; do
      [[ -n "${_col_names[$_i]}" ]] || continue
      _col_ref="${_nn_tbl}.${_col_names[$_i]}"
      _pg_type=$(case "${_col_dtypes[$_i]}" in
        tinyint)   [[ "${_col_ctypes[$_i]}" == "tinyint(1)" ]] && echo "boolean drop typemod using tinyint-to-boolean" || echo "smallint drop typemod" ;;
        smallint|year) echo "smallint drop typemod" ;;
        mediumint|int) echo "integer drop typemod" ;;
        bigint)    echo "bigint drop typemod" ;;
        float)     echo "real drop typemod" ;;
        double)    echo "double precision drop typemod" ;;
        decimal|numeric) echo "numeric" ;;
        date)      echo "date drop typemod" ;;
        datetime)  echo "timestamp drop typemod" ;;
        timestamp) echo "timestamptz drop typemod" ;;
        time)      echo "time" ;;
        json)      echo "json drop typemod" ;;
        *blob|binary|varbinary) echo "bytea drop typemod" ;;
        *)         echo "text drop typemod" ;;
      esac)
      _emit_nn_rule "$_col_ref" "$_pg_type" "large table, skipped scan"
    done
    continue
  fi

  # Small/medium table: one MAX(c IS NULL),... query — single table scan.
  _select_parts=()
  for _cn in "${_col_names[@]}"; do
    [[ -n "$_cn" ]] || continue
    _select_parts+=("MAX(\`${_cn}\` IS NULL)")
  done
  [[ ${#_select_parts[@]} -eq 0 ]] && continue

  _null_row=$(mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED -N -B \
    -e "SELECT $(IFS=','; echo "${_select_parts[*]}") FROM \`${_nn_tbl}\`" \
    "$MYSQL_DB" 2>/dev/null || true)
  [[ -z "$_null_row" ]] && continue

  IFS=$'\t' read -ra _null_flags <<< "$_null_row"
  for _i in "${!_col_names[@]}"; do
    [[ -n "${_col_names[$_i]}" ]] || continue
    [[ "${_null_flags[$_i]:-0}" == "1" ]] || continue
    _col_ref="${_nn_tbl}.${_col_names[$_i]}"
    _pg_type=$(case "${_col_dtypes[$_i]}" in
      tinyint)   [[ "${_col_ctypes[$_i]}" == "tinyint(1)" ]] && echo "boolean drop typemod using tinyint-to-boolean" || echo "smallint drop typemod" ;;
      smallint|year) echo "smallint drop typemod" ;;
      mediumint|int) echo "integer drop typemod" ;;
      bigint)    echo "bigint drop typemod" ;;
      float)     echo "real drop typemod" ;;
      double)    echo "double precision drop typemod" ;;
      decimal|numeric) echo "numeric" ;;
      date)      echo "date drop typemod" ;;
      datetime)  echo "timestamp drop typemod" ;;
      timestamp) echo "timestamptz drop typemod" ;;
      time)      echo "time" ;;
      json)      echo "json drop typemod" ;;
      *blob|binary|varbinary) echo "bytea drop typemod" ;;
      *)         echo "text drop typemod" ;;
    esac)
    _emit_nn_rule "$_col_ref" "$_pg_type" "null found"
  done
done

if [[ ${#_nn_skipped_tables[@]} -gt 0 ]]; then
  echo "  NOTE: ${#_nn_skipped_tables[@]} large table(s) skipped data scan; all NOT NULL cols got 'drop not null' proactively:"
  for _st in "${_nn_skipped_tables[@]}"; do echo "    ${_st}"; done
  echo "  To scan large tables too: set PGLOADER_NULL_SCAN_THRESHOLD=0"
fi
if [[ $_nn_violation_count -eq 0 ]]; then
  echo "  (none found)"
else
  echo "==> ${_nn_violation_count} NOT NULL column rule(s) emitted"
fi

# Base type rules (column rules above take priority).
CAST_RULES+=(
  "type tinyint when (= 1 precision) to boolean drop typemod using tinyint-to-boolean,
  type int with extra auto_increment to serial,
  type bigint with extra auto_increment to bigserial,
  type bigint when unsigned to numeric drop typemod"
)

CAST_BLOCK="CAST"
for _i in "${!CAST_RULES[@]}"; do
  _sep=$([[ $_i -lt $((${#CAST_RULES[@]}-1)) ]] && printf ',' || printf '')
  CAST_BLOCK+=$'\n'"  ${CAST_RULES[$_i]}${_sep}"
done

LOAD_FILE="/tmp/pgloader_migrate_${TS}.load"
PGPASS_FILE="/tmp/.pgpass_migrate_${TS}"
trap 'rm -f "$LOAD_FILE" "$PGPASS_FILE"' EXIT
umask 077

# .pgpass avoids embedding special-char passwords in the URI
printf '%s:%s:%s:%s:%s\n' "$PG_FQDN" "5432" "$PG_DB" "$PG_USER" "$PG_PASSWORD" > "$PGPASS_FILE"

cat > "$LOAD_FILE" <<EOF
LOAD DATABASE
  FROM ${MYSQL_URI}
  INTO ${PG_URI}

ALTER TABLE NAMES MATCHING ~/./ SET SCHEMA '${PG_SCHEMA}'

WITH
  include drop,
  create tables,
  create indexes,
  drop indexes,
  foreign keys,
  reset sequences,
  workers = ${WORKERS},
  concurrency = ${CONCURRENCY},
  max parallel create index = 2,
  batch rows = 2000,
  prefetch rows = 3000,
  batch size = 16MB

SET MySQL PARAMETERS
  net_read_timeout TO '${NET_READ_TIMEOUT}',
  net_write_timeout TO '${NET_WRITE_TIMEOUT}',
  wait_timeout TO '86400',
  interactive_timeout TO '86400'

SET PostgreSQL PARAMETERS
  work_mem TO '16MB',
  maintenance_work_mem TO '256MB'

${CAST_BLOCK}

EXCLUDING TABLE NAMES MATCHING 'flyway_schema_history', 'schema_migrations'${EXCLUDE_TABLES_CLAUSE}
;
EOF

echo "=== pgloader migration (docker) — $(date -u) ==="
echo "  ${MYSQL_FQDN}/${MYSQL_DB} → ${PG_FQDN}/${PG_DB} (schema: ${PG_SCHEMA})"
echo "  workers=${WORKERS} concurrency=${CONCURRENCY}"
echo "  log: ${LOG_FILE}"
echo "  WARNING: include drop will recreate target tables."
echo ""

# Attempt to raise MySQL connection timeouts at the server level.
# pgloader opens one connection per worker; the default MySQL net_write_timeout (60s)
# kills any connection that stalls mid-result-set (common with large tables + high parallelism).
# This requires SUPER or SYSTEM_VARIABLES_ADMIN privilege; failure is non-fatal.
echo "==> Setting MySQL global timeouts (net_read_timeout=${NET_READ_TIMEOUT}s, net_write_timeout=${NET_WRITE_TIMEOUT}s)..."
mysql -h "$MYSQL_FQDN" -u "$MYSQL_USER" --ssl-mode=REQUIRED \
  -e "SET GLOBAL net_read_timeout=${NET_READ_TIMEOUT}; SET GLOBAL net_write_timeout=${NET_WRITE_TIMEOUT};" \
  "$MYSQL_DB" 2>/dev/null \
  && echo "  OK" \
  || echo "  NOTE: Could not set MySQL global timeouts (needs SUPER/SYSTEM_VARIABLES_ADMIN). Set net_read_timeout=${NET_READ_TIMEOUT} and net_write_timeout=${NET_WRITE_TIMEOUT} in Azure MySQL server parameters."
echo ""

ERRORS_DIR="${LOG_DIR}/errors-${TS}"
mkdir -p "$ERRORS_DIR"

PGLOADER_RC=0
docker run --rm --network host \
  -v /etc/ssl/certs:/etc/ssl/certs:ro \
  -e SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
  -e SSL_CERT_DIR=/etc/ssl/certs \
  -v "${PGPASS_FILE}:/root/.pgpass:ro" \
  -v "${LOAD_FILE}:/tmp/migrate.load:ro" \
  -v "${LOG_DIR}:/migration-logs" \
  -v "${ERRORS_DIR}:/migration-errors" \
  dimitri/pgloader:latest \
  pgloader --verbose --root-dir /migration-errors \
    --logfile /migration-logs/pgloader-detail-${TS}.log \
    /tmp/migrate.load \
  < /dev/null 2>&1 | tee "$LOG_FILE" || PGLOADER_RC=$?

echo ""
echo "=== pgloader finished — $(date -u) (exit code: ${PGLOADER_RC}) ==="

# Report any rows dropped to the error log (binary type mismatches, constraint violations, etc.)
_err_files=$(find "${ERRORS_DIR}" -maxdepth 1 \( -name "*.dat" -o -name "*.log" \) 2>/dev/null | sort)
if [[ -n "$_err_files" ]]; then
  echo ""
  echo "  WARNING: pgloader dropped some rows — inspect error files in: ${ERRORS_DIR}"
  ls -lh "${ERRORS_DIR}/" 2>/dev/null | grep -v '^total' | sed 's/^/    /'
fi

# grep -c always outputs a count (even 0) and exits 1 when no matches; '|| true' avoids
# appending a second '0' that would make _trunc_count a multiline string.
_trunc_count=$(grep -c "will be truncated to" "$LOG_FILE" 2>/dev/null || true)
if [[ "$_trunc_count" -gt 0 ]]; then
  echo ""
  echo "  NOTE: ${_trunc_count} index name(s) exceeded PostgreSQL's 63-char identifier limit and were truncated."
  echo "  Risk: if two long names truncate to the same string, one index is silently dropped."
  echo "  Verify with the compare script, or: grep 'truncated to' ${LOG_FILE}"
fi

# pgloader leaves an empty schema named after the MySQL source DB after moving all tables to '${PG_SCHEMA}'.
echo ""
echo "  If you're migrating to the [public] schema, pgloader leaves an empty '${MYSQL_DB}' schema after moving all tables to '${PG_SCHEMA}'."
_prompt_read "==> Drop that schema from ${PG_DB}? [Y/n] " _ds_ans
if [[ "${_ds_ans,,}" != "n" ]]; then
  echo "==> Dropping schema '${MYSQL_DB}'..."
  if ! psql -U "$PG_USER" -d "$PG_DB" --no-password \
         -c "DROP SCHEMA IF EXISTS \"${MYSQL_DB}\" CASCADE;" 2>&1; then
    echo "  WARNING: Could not drop schema '${MYSQL_DB}' — inspect manually."
  else
    echo "==> Done."
  fi
else
  echo "==> Skipped — schema '${MYSQL_DB}' left in place."
fi

echo ""
_prompt_read "==> Run VACUUM ANALYZE on ${PG_DB}? [Y/n] " _va_ans
if [[ "${_va_ans,,}" != "n" ]]; then
  echo "==> Running VACUUM ANALYZE on ${PG_FQDN}/${PG_DB} — this may take a while..."
  psql -U "$PG_USER" -d "$PG_DB" --no-password -c "VACUUM ANALYZE;"
  echo "==> VACUUM ANALYZE complete — $(date -u)"
fi

_elapsed=$(( $(date +%s) - _job_start ))
printf '\n==> Total time: %02dh %02dm %02ds\n' \
  $(( _elapsed/3600 )) $(( (_elapsed%3600)/60 )) $(( _elapsed%60 ))

exit "$PGLOADER_RC"

#!/usr/bin/env bash
# Compare MySQL source vs PostgreSQL target: inventory, row counts, columns,
# PK fingerprints (COUNT/MIN/MAX), AUTO_INCREMENT vs sequences, and FK counts.
#
# PostgreSQL side: database = PG_DB; compares tables in PG_SCHEMA (default public).
#
# Usage:
#   bash 3-compare-mysql-pg.sh --env-file ./migration.env
#   bash 3-compare-mysql-pg.sh \
#     --mysql-host mydb.mysql.database.azure.com --mysql-user migrate_admin --mysql-db the_db \
#     --pg-host mypg.postgres.database.azure.com --pg-user psqladmin --pg-db some_db --pg-schema public
#
# Options:
#   --mysql-host HOST        MySQL FQDN (or env MYSQL_FQDN)
#   --mysql-user USER        MySQL user (or env MYSQL_USER)
#   --mysql-db DB            MySQL database (or env MYSQL_DB)
#   --mysql-pass-file FILE   Read MySQL password from FILE (or env MYSQL_PASSWORD)
#   --pg-host HOST           PostgreSQL FQDN (or env PG_FQDN)
#   --pg-user USER           PostgreSQL user (or env PG_USER)
#   --pg-db DB               PostgreSQL database (or env PG_DB)
#   --pg-pass-file FILE      Read PG password from FILE (or env PG_PASSWORD)
#   --pg-schema SCHEMA       PostgreSQL schema (default: public)
#   --env-file FILE          Source variables from FILE
#   -q, --quiet              Show only mismatches and summary (skip column details)
#   -F, --fast               Fast mode: estimated counts, batch checks, no per-table details
#   -z, --zero-dates         Check DATE/DATETIME/TIMESTAMP columns for zero-date values
#
# Optional env:
#   PG_SCHEMA=public
#   LOG_DIR=~/migration/logs
#   COMPARE_FINGERPRINT_MAX_ROWS=50000000

set -euo pipefail

# --- Helpers ---
read_password_file() {
  local f="$1"
  [[ -f "$f" && -r "$f" ]] || { echo "Cannot read password file: $f" >&2; return 1; }
  head -1 "$f" | tr -d '\n'
}

# --- Argument parsing ---
ENV_FILE=""
CLI_MYSQL_HOST="" CLI_MYSQL_USER="" CLI_MYSQL_DB="" CLI_MYSQL_PASS_FILE=""
CLI_PG_HOST="" CLI_PG_USER="" CLI_PG_DB="" CLI_PG_PASS_FILE=""
CLI_PG_SCHEMA=""
QUIET=0
FAST=0
CHECK_ZERO_DATES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mysql-host)       CLI_MYSQL_HOST="$2"; shift 2 ;;
    --mysql-user)       CLI_MYSQL_USER="$2"; shift 2 ;;
    --mysql-db)         CLI_MYSQL_DB="$2"; shift 2 ;;
    --mysql-pass-file)  CLI_MYSQL_PASS_FILE="$2"; shift 2 ;;
    --pg-host)          CLI_PG_HOST="$2"; shift 2 ;;
    --pg-user)          CLI_PG_USER="$2"; shift 2 ;;
    --pg-db)            CLI_PG_DB="$2"; shift 2 ;;
    --pg-pass-file)     CLI_PG_PASS_FILE="$2"; shift 2 ;;
    --pg-schema)        CLI_PG_SCHEMA="$2"; shift 2 ;;
    --env-file)         ENV_FILE="$2"; shift 2 ;;
    -q|--quiet)         QUIET=1; shift ;;
    -F|--fast)          FAST=1; QUIET=1; shift ;;
    -z|--zero-dates)    CHECK_ZERO_DATES=1; shift ;;
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
[[ -n "$CLI_PG_SCHEMA" ]]  && PG_SCHEMA="$CLI_PG_SCHEMA"

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

PG_SCHEMA="${PG_SCHEMA:-public}"
COMPARE_FINGERPRINT_MAX_ROWS="${COMPARE_FINGERPRINT_MAX_ROWS:-999999999999999}"

LOG_DIR="${LOG_DIR:-${HOME}/migration/logs}"
TS=$(date -u +"%Y%m%d_%H%M%S")
mkdir -p "$LOG_DIR"
REPORT="${LOG_DIR}/compare-mysql-pg_${TS}.txt"

# --- Query wrappers & helpers ---
my_q() { mysql --defaults-file="$MYCNF" -D "$MYSQL_DB" -N -e "$1" 2>/dev/null; }
pg_q() { psql --username="$PG_USER" --dbname="$PG_DB" --no-password -At -c "$1" 2>/dev/null; }
fail()  { EXIT=1; FAILURES+=("$1"); }

mysql_ident()  { local t="${1//\`/\`\`}"; printf '`%s`' "$t"; }
pg_ident()     { printf '"%s"."%s"' "${1//\"/\"\"}" "${2//\"/\"\"}"; }
pg_ident_col() { printf '"%s"' "${1//\"/\"\"}"; }
sql_literal()  { printf '%s' "${1//\'/\'\'}"; }

is_excluded_table() {
  case "$1" in flyway_schema_history|schema_migrations) return 0 ;; *) return 1 ;; esac
}

# ─── MySQL → PostgreSQL type mapping ─────────────────────────────────────────
# Maps MySQL DATA_TYPE + COLUMN_TYPE to the expected PostgreSQL udt_name as it
# appears in information_schema.columns after migration.  serial4/serial8 are
# syntactic sugar — their udt_name is still int4/int8, so auto_increment columns
# do not need special handling here.
# Returns a space-separated list when multiple PG types are acceptable (e.g.
# MySQL timestamp can map to 'timestamp' or 'timestamptz' depending on the tool).
# Returns empty string for unknown types — comparison is skipped for that column.
mysql_type_to_pg_udt() {
  local dtype="$1" ctype="$2"
  local len=""
  [[ "$ctype" =~ \(([0-9]+) ]] && len="${BASH_REMATCH[1]}"
  case "$dtype" in
    tinyint)   [[ "$ctype" == "tinyint(1)" ]] && echo "bool" || echo "int2" ;;
    smallint|year) echo "int2" ;;
    mediumint) echo "int4" ;;
    int)       [[ "$ctype" == "int unsigned" ]] && echo "int8" || echo "int4" ;;
    bigint)    [[ "$ctype" == "bigint unsigned" ]] && echo "numeric" || echo "int8" ;;
    float)     echo "float4" ;;
    double)    echo "float8" ;;
    decimal|numeric) echo "numeric" ;;
    char)      [[ -n "$len" ]] && echo "bpchar(${len})" || echo "bpchar" ;;
    varchar)   [[ -n "$len" ]] && echo "varchar(${len})" || echo "varchar" ;;
    tinytext|mediumtext|longtext|text) echo "text" ;;
    date)      echo "date" ;;
    datetime)  echo "timestamptz" ;;
    timestamp) echo "timestamptz" ;;
    time)      echo "time" ;;
    json)      echo "json jsonb" ;;
    tinyblob|blob|mediumblob|longblob|binary|varbinary) echo "bytea" ;;
    bit)       [[ "$ctype" == "bit(1)" ]] && echo "bool" || echo "bit" ;;
    enum|set)  echo "text" ;;
    *)         echo "" ;;
  esac
}

# ─── Zero-date check ─────────────────────────────────────────────────────────
# Compares MySQL zero-date values ('0000-00-00') against PostgreSQL NULL counts.
# pg_chameleon converts zero-dates to NULL during migration; this verifies the
# conversion happened correctly and no rows were silently discarded.
#
# STATUS meanings:
#   OK       PG NULLs >= MySQL zeros + MySQL NULLs (all zero-dates accounted for)
#   PARTIAL  PG NULLs >= MySQL NULLs but < MySQL zeros + MySQL NULLs
#            (some zero-date rows may have been discarded during migration)
#   MISMATCH PG NULLs < MySQL NULLs (legitimate NULLs are also missing)
#   ERR      Query failed (table may not exist in PG or column type mismatch)
check_zero_dates() {
  echo ""
  echo "--- Zero-date values (MySQL '0000-00-00' -> PostgreSQL NULL) ---"
  echo "  Checks DATE/DATETIME/TIMESTAMP columns for MySQL zero-date values."
  echo "  MY_ZEROS: rows with '0000-00-00'; MY_NULLS: MySQL NULL rows;"
  echo "  PG_NULLS: PostgreSQL NULL rows (expected >= MY_ZEROS + MY_NULLS after migration)."
  printf '\n%-40s %-24s %-12s %10s %10s %10s  %s\n' \
    "TABLE" "COLUMN" "TYPE" "MY_ZEROS" "MY_NULLS" "PG_NULLS" "STATUS"
  printf '%s\n' "$(printf -- '-%.0s' {1..112})"

  local _zd_found=0

  local all_date_cols
  all_date_cols=$(my_q "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA='$(sql_literal "${MYSQL_DB}")'
      AND DATA_TYPE IN ('date','datetime','timestamp')
    ORDER BY TABLE_NAME, ORDINAL_POSITION" || true)

  if [[ -z "$all_date_cols" ]]; then
    echo "  (none -- no date/datetime/timestamp columns found in MySQL)"
    return
  fi

  local tbl col dtype ti pi mi pci has_zeros counts zero_count mysql_nulls pg_nulls expected status shortfall
  while IFS=$'\t' read -r tbl col dtype; do
    [[ -z "$tbl" || -z "$col" ]] && continue
    is_excluded_table "$tbl" && continue
    # Skip tables not present in both MySQL and PostgreSQL
    grep -qx "$tbl" "$TMPD/both.txt" 2>/dev/null || continue

    ti=$(mysql_ident "$tbl")
    pi=$(pg_ident "$PG_SCHEMA" "$tbl")
    mi=$(mysql_ident "$col")
    pci=$(pg_ident_col "$col")

    # Quick existence check — avoids a full COUNT(*) scan when no zero-date values exist
    has_zeros=$(my_q "SELECT 1 FROM ${ti} WHERE ${mi} IS NOT NULL AND CAST(${mi} AS CHAR) LIKE '0000-00-00%' LIMIT 1" || true)
    [[ -z "$has_zeros" ]] && continue

    # One scan per column: count zero-dates and NULLs together
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

    _zd_found=$(( _zd_found + 1 ))

    pg_nulls=$(pg_q "SELECT COUNT(*) FROM ${pi} WHERE ${pci} IS NULL" || echo "ERR")
    expected=$(( zero_count + mysql_nulls ))
    shortfall=0

    if [[ "$pg_nulls" == "ERR" || ! "$pg_nulls" =~ ^[0-9]+$ ]]; then
      status="ERR"
    elif [[ "$pg_nulls" -ge "$expected" ]]; then
      status="OK"
    elif [[ "$pg_nulls" -ge "$mysql_nulls" ]]; then
      shortfall=$(( expected - pg_nulls ))
      status="PARTIAL"
      fail "Zero-date partial: ${tbl}.${col} (${shortfall} zero-date row(s) may be missing or discarded)"
    else
      status="MISMATCH"
      fail "Zero-date mismatch: ${tbl}.${col} (expected PG NULL>=${expected}, got ${pg_nulls})"
    fi

    printf '%-40s %-24s %-12s %10s %10s %10s  %s\n' \
      "$tbl" "$col" "$dtype" "$zero_count" "$mysql_nulls" "$pg_nulls" "$status"

  done <<< "$all_date_cols"

  if [[ "$_zd_found" -eq 0 ]]; then
    echo "  (none -- no zero-date values found in MySQL date/datetime/timestamp columns)"
  fi
}

MYCNF="$(mktemp /tmp/.my_compare_XXXXXX.cnf)"
chmod 600 "$MYCNF"
cat >"$MYCNF" <<MYCNF
[client]
host=${MYSQL_FQDN}
user=${MYSQL_USER}
password=${MYSQL_PASSWORD}
database=${MYSQL_DB}
ssl-mode=REQUIRED
MYCNF

TMPD="$(mktemp -d /tmp/compare_mpg_XXXXXX)"
trap 'rm -rf "$TMPD"; rm -f "$MYCNF"' EXIT

# Use a FIFO for tee so no pipe-buffer data is lost (process substitution is unreliable).
_FIFO="$TMPD/tee_fifo"
mkfifo "$_FIFO"
tee "$REPORT" < "$_FIFO" &
TEE_PID=$!
exec > "$_FIFO" 2>&1

echo "================================================================"
if [[ $FAST -eq 1 ]]; then
  echo " MySQL vs PostgreSQL comparison — FAST MODE — $(date -u)"
else
  echo " MySQL vs PostgreSQL comparison — $(date -u)"
fi
echo " MySQL: ${MYSQL_FQDN} / ${MYSQL_DB}"
echo " PostgreSQL: ${PG_FQDN} / ${PG_DB} / schema ${PG_SCHEMA}"
echo " Report file: ${REPORT}"
echo "================================================================"

FAILURES=()
EXIT=0

# --- Build table lists ---
my_q "SELECT TABLE_NAME FROM information_schema.TABLES
      WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_TYPE='BASE TABLE'
      ORDER BY TABLE_NAME" >"$TMPD/mysql_tables.txt"

pg_q "SELECT table_name FROM information_schema.tables
      WHERE table_schema='${PG_SCHEMA}' AND table_type='BASE TABLE'
      ORDER BY 1" >"$TMPD/pg_tables.txt"

sort "$TMPD/mysql_tables.txt" >"$TMPD/m.s"
sort "$TMPD/pg_tables.txt"    >"$TMPD/p.s"
comm -12 "$TMPD/m.s" "$TMPD/p.s" >"$TMPD/both.txt"
comm -23 "$TMPD/m.s" "$TMPD/p.s" >"$TMPD/mysql_only.txt"
comm -13 "$TMPD/m.s" "$TMPD/p.s" >"$TMPD/pg_only.txt"

echo ""
echo "--- Tables only on MySQL (missing on PostgreSQL) ---"
if [[ -s "$TMPD/mysql_only.txt" ]]; then
  cat "$TMPD/mysql_only.txt"
  while IFS= read -r _t; do fail "Missing on PG: ${_t}"; done <"$TMPD/mysql_only.txt"
else
  echo "(none)"
fi

echo ""
echo "--- Tables only on PostgreSQL (not on MySQL) ---"
if [[ -s "$TMPD/pg_only.txt" ]]; then cat "$TMPD/pg_only.txt"; else echo "(none)"; fi

# --- Foreign keys ---
echo ""
echo "--- Foreign keys (information_schema) ---"
mysql_fk=$(my_q "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
                 WHERE CONSTRAINT_SCHEMA='${MYSQL_DB}' AND CONSTRAINT_TYPE='FOREIGN KEY'" || echo "ERR")
pg_fk=$(pg_q "SELECT COUNT(*) FROM information_schema.table_constraints
              WHERE table_schema='${PG_SCHEMA}' AND constraint_type='FOREIGN KEY'" || echo "ERR")
printf 'MySQL FK constraints: %s\n' "$mysql_fk"
printf 'PostgreSQL FK constraints (%s): %s\n' "$PG_SCHEMA" "$pg_fk"
if [[ "$mysql_fk" != "ERR" && "$pg_fk" != "ERR" && "$mysql_fk" != "$pg_fk" ]]; then
  echo "NOTE: FK counts differ — review migrations / excluded tables."
  fail "FK count mismatch: MySQL=${mysql_fk} PG=${pg_fk}"
fi

# ═══════════════════════════════════════════════════════════════════════
# FAST MODE: estimated row counts + batch sequence check → early exit
# ═══════════════════════════════════════════════════════════════════════
if [[ $FAST -eq 1 ]]; then

  # --- Row counts (estimated) ---
  echo ""
  echo "--- Row counts (estimated — fast mode) ---"
  echo "NOTE: MySQL uses InnoDB TABLE_ROWS estimates, PG uses pg_class.reltuples."
  echo "      Counts may differ slightly from exact COUNT(*).  Re-run without --fast for exact."

  # Batch: all MySQL estimated counts in one query
  my_q "SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.TABLES
        WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_TYPE='BASE TABLE'
        ORDER BY TABLE_NAME" > "$TMPD/my_est.txt" || true

  # Batch: all PG estimated counts in one query
  pg_q "SELECT c.relname, c.reltuples::bigint
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = '${PG_SCHEMA}' AND c.relkind = 'r'
        ORDER BY c.relname" > "$TMPD/pg_est.txt" || true

  declare -A MY_EST PG_EST
  while IFS=$'\t' read -r _t _c; do
    [[ -n "$_t" ]] && MY_EST["$_t"]="$_c"
  done < "$TMPD/my_est.txt"
  while IFS='|' read -r _t _c; do
    [[ -n "$_t" ]] && PG_EST["$_t"]="$_c"
  done < "$TMPD/pg_est.txt"

  printf '\n%-50s %15s %15s %12s\n' "TABLE" "MYSQL_EST" "PG_EST" "STATUS"
  printf '%s\n' "$(printf -- '-%.0s' {1..92})"

  VERIFY_TABLES=()
  while IFS= read -r tbl || [[ -n "$tbl" ]]; do
    [[ -z "$tbl" ]] && continue
    is_excluded_table "$tbl" && continue
    m="${MY_EST[$tbl]:-0}"
    p="${PG_EST[$tbl]:-0}"
    # Flag if estimates differ by > 0.5% or > 50 rows
    if [[ "$m" =~ ^[0-9]+$ && "$p" =~ ^[0-9]+$ ]]; then
      diff_abs=$(( m > p ? m - p : p - m ))
      threshold=$(( m / 200 ))
      (( threshold < 50 )) && threshold=50
      if (( diff_abs > threshold )); then
        printf '%-50s %15s %15s %12s\n' "$tbl" "$m" "$p" "VERIFY"
        VERIFY_TABLES+=("$tbl")
      else
        printf '%-50s %15s %15s %12s\n' "$tbl" "$m" "$p" "~ok"
      fi
    else
      printf '%-50s %15s %15s %12s\n' "$tbl" "$m" "$p" "ERR"
    fi
  done < "$TMPD/both.txt"

  # Verify flagged tables with exact COUNT(*)
  if [[ ${#VERIFY_TABLES[@]} -gt 0 ]]; then
    echo ""
    echo "--- Verifying ${#VERIFY_TABLES[@]} table(s) with exact COUNT(*) ---"
    printf '%-50s %15s %15s %10s\n' "TABLE" "MYSQL" "PG" "DIFF"
    printf '%s\n' "$(printf -- '-%.0s' {1..92})"

    # Build batch UNION ALL for MySQL
    _sql=""
    for _t in "${VERIFY_TABLES[@]}"; do
      _mi=$(mysql_ident "$_t")
      [[ -n "$_sql" ]] && _sql+=" UNION ALL "
      _sql+="SELECT '$(sql_literal "$_t")' t, COUNT(*) c FROM ${_mi}"
    done
    my_q "$_sql" > "$TMPD/my_exact.txt" || true

    # Build batch UNION ALL for PG
    _sql=""
    for _t in "${VERIFY_TABLES[@]}"; do
      _pi=$(pg_ident "$PG_SCHEMA" "$_t")
      [[ -n "$_sql" ]] && _sql+=" UNION ALL "
      _sql+="SELECT '$(sql_literal "$_t")' t, COUNT(*) c FROM ${_pi}"
    done
    pg_q "$_sql" > "$TMPD/pg_exact.txt" || true

    declare -A MY_EX PG_EX
    while IFS=$'\t' read -r _t _c; do
      [[ -n "$_t" ]] && MY_EX["$_t"]="$_c"
    done < "$TMPD/my_exact.txt"
    while IFS='|' read -r _t _c; do
      [[ -n "$_t" ]] && PG_EX["$_t"]="$_c"
    done < "$TMPD/pg_exact.txt"

    for _t in "${VERIFY_TABLES[@]}"; do
      _m="${MY_EX[$_t]:-ERR}"
      _p="${PG_EX[$_t]:-ERR}"
      if [[ "$_m" == "$_p" && "$_m" != "ERR" ]]; then
        printf '%-50s %15s %15s %10s\n' "$_t" "$_m" "$_p" "OK"
      else
        _d="ERR"
        [[ "$_m" =~ ^[0-9]+$ && "$_p" =~ ^[0-9]+$ ]] && _d=$(( _m - _p ))
        printf '%-50s %15s %15s %+10s\n' "$_t" "$_m" "$_p" "$_d"
        fail "Row count mismatch: ${_t} (MySQL=${_m} PG=${_p})"
      fi
    done
  fi

  # --- Batch sequence check ---
  echo ""
  echo "--- Sequences ---"

  # Get MySQL AUTO_INCREMENT columns (batch, instant)
  my_q "SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA='${MYSQL_DB}' AND EXTRA LIKE '%auto_increment%'
        ORDER BY TABLE_NAME" > "$TMPD/my_ai.txt" || true

  # Get PG columns with sequence defaults (batch, instant)
  pg_q "SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema='${PG_SCHEMA}'
          AND (column_default LIKE 'nextval%' OR is_identity = 'YES')
        ORDER BY table_name" > "$TMPD/pg_seqcols.txt" || true

  # Get all PG sequence last_values (batch, instant)
  pg_q "SELECT schemaname||'.'||sequencename, last_value FROM pg_sequences" > "$TMPD/pg_seqvals.txt" || true

  # Build sets for comparison
  declare -A PG_HAS_SEQ
  while IFS='|' read -r _t _c; do
    [[ -n "$_t" ]] && PG_HAS_SEQ["${_t}|${_c}"]=1
  done < "$TMPD/pg_seqcols.txt"

  _missing_seq=0
  _behind_seq=0
  while IFS=$'\t' read -r _t _c; do
    [[ -z "$_t" ]] && continue
    is_excluded_table "$_t" && continue
    if [[ -z "${PG_HAS_SEQ[${_t}|${_c}]:-}" ]]; then
      echo "  >>> Missing sequence: ${_t}.${_c} (MySQL AUTO_INCREMENT with no PG sequence/default)"
      fail "Missing sequence: ${_t}.${_c} (MySQL AUTO_INCREMENT with no PG sequence/default)"
      ((_missing_seq++)) || true
    fi
  done < "$TMPD/my_ai.txt"

  # Check for sequences behind MAX(pk) — build batch MAX query
  if [[ -s "$TMPD/pg_seqcols.txt" ]]; then
    _sql=""
    _seq_tables=()
    _seq_cols=()
    while IFS='|' read -r _t _c; do
      [[ -z "$_t" ]] && continue
      is_excluded_table "$_t" && continue
      _pi=$(pg_ident "$PG_SCHEMA" "$_t")
      _pci=$(pg_ident_col "$_c")
      [[ -n "$_sql" ]] && _sql+=" UNION ALL "
      _sql+="SELECT '$(sql_literal "$_t")|$(sql_literal "$_c")' k, COALESCE(MAX(${_pci})::text,'0') v FROM ${_pi}"
      _seq_tables+=("$_t")
      _seq_cols+=("$_c")
    done < "$TMPD/pg_seqcols.txt"

    if [[ -n "$_sql" ]]; then
      pg_q "$_sql" > "$TMPD/pg_maxpk.txt" || true

      declare -A PG_MAXPK
      while IFS='|' read -r _k _v; do
        [[ -n "$_k" ]] && PG_MAXPK["$_k"]="$_v"
      done < "$TMPD/pg_maxpk.txt"

      for _i in "${!_seq_tables[@]}"; do
        _t="${_seq_tables[$_i]}"
        _c="${_seq_cols[$_i]}"
        _max="${PG_MAXPK[${_t}|${_c}]:-0}"
        _seq=$(pg_q "SELECT pg_get_serial_sequence('\"${PG_SCHEMA}\".\"$(sql_literal "$_t")\"','$(sql_literal "$_c")')" 2>/dev/null || true)
        if [[ -n "$_seq" ]]; then
          _lv=$(grep "^${_seq}|" "$TMPD/pg_seqvals.txt" 2>/dev/null | cut -d'|' -f2 || true)
          if [[ "$_lv" =~ ^[0-9]+$ && "$_max" =~ ^[0-9]+$ && "$_lv" -lt "$_max" ]]; then
            echo "  >>> Sequence behind: ${_t}.${_c} seq=${_seq} last_value=${_lv} MAX=${_max}"
            fail "Sequence behind: ${_t}.${_c} (last_value=${_lv} < MAX=${_max})"
            ((_behind_seq++)) || true
          fi
        fi
      done
    fi
  fi

  echo "  Missing sequences: ${_missing_seq}   Sequences behind: ${_behind_seq}"

  [[ $CHECK_ZERO_DATES -eq 1 ]] && check_zero_dates

  # --- Summary ---
  echo ""
  echo "================================================================"
  if [[ "$EXIT" -eq 0 ]]; then
    echo "Summary: ALL CHECKS PASSED — no mismatches found."
  else
    echo "Summary: ${#FAILURES[@]} issue(s) found:"
    echo ""
    for i in "${!FAILURES[@]}"; do
      printf '  %2d. %s\n' "$((i+1))" "${FAILURES[$i]}"
    done
  fi
  echo "================================================================"
  echo ""
  echo "Full report: ${REPORT}"

  exec 1>&- 2>&-
  wait "$TEE_PID" 2>/dev/null || true
  exit "$EXIT"
fi

# ═══════════════════════════════════════════════════════════════════════
# NORMAL MODE (original per-table behaviour)
# ═══════════════════════════════════════════════════════════════════════

# --- Row counts ---
echo ""
echo "--- Row counts (migration metadata tables skipped) ---"
printf '%-40s %15s %15s %8s\n' "TABLE" "MYSQL_ROWS" "PG_ROWS" "MATCH"
printf '%s\n' "--------------------------------------------------------------------------------"
declare -A ROWCNT_MYSQL ROWCNT_PG

while IFS= read -r tbl || [[ -n "$tbl" ]]; do
  [[ -z "$tbl" ]] && continue
  is_excluded_table "$tbl" && continue
  mi=$(mysql_ident "$tbl")
  pi=$(pg_ident "$PG_SCHEMA" "$tbl")
  mysql_cnt=$(my_q "SELECT COUNT(*) FROM ${mi}" || echo "ERR")
  pg_cnt=$(pg_q "SELECT COUNT(*) FROM ${pi}"    || echo "ERR")
  ROWCNT_MYSQL["$tbl"]="$mysql_cnt"
  ROWCNT_PG["$tbl"]="$pg_cnt"
  if [[ "$mysql_cnt" == "$pg_cnt" && "$mysql_cnt" != "ERR" ]]; then
    match="YES"
  else
    match="NO"; fail "Row count mismatch: ${tbl} (MySQL=${mysql_cnt} PG=${pg_cnt})"
  fi
  printf '%-40s %15s %15s %8s\n' "$tbl" "$mysql_cnt" "$pg_cnt" "$match"
done <"$TMPD/both.txt"

# --- Per-table: columns, PK fingerprint, sequences ---
echo ""
if [[ $QUIET -eq 0 ]]; then
  echo "--- Per-table: columns, PK fingerprint, AUTO_INCREMENT vs sequence ---"
  echo "(PK fingerprint = COUNT,MIN,MAX on single-column PKs with matching names.)"
  echo ""
fi

while IFS= read -r tbl || [[ -n "$tbl" ]]; do
  [[ -z "$tbl" ]] && continue
  is_excluded_table "$tbl" && continue
  ts=$(sql_literal "$tbl")

  if [[ $QUIET -eq 0 ]]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "TABLE: ${tbl}"
    echo ""
  fi

  my_q "SELECT CONCAT(COLUMN_NAME,'|',COLUMN_TYPE,'|',IFNULL(COLUMN_KEY,''),'|',IFNULL(EXTRA,''))
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_NAME='${ts}'
        ORDER BY COLUMN_NAME" | sort >"$TMPD/mnorm_${tbl}.txt" || true

  pg_q "SELECT column_name||'|'||udt_name||' ('||data_type||')'||'|'||
               COALESCE(replace(column_default,'|','/'),'')
               ||'|'||COALESCE(is_identity,'NO')
        FROM information_schema.columns
        WHERE table_schema='${PG_SCHEMA}' AND table_name='${ts}'
        ORDER BY column_name" | sort >"$TMPD/pnorm_${tbl}.txt" || true

  if ! cmp -s <(cut -d'|' -f1 "$TMPD/mnorm_${tbl}.txt" | sort) \
              <(cut -d'|' -f1 "$TMPD/pnorm_${tbl}.txt" | sort) 2>/dev/null; then
    echo "  >>> Column NAME sets differ (MySQL-only / PG-only):"
    comm -3 <(cut -d'|' -f1 "$TMPD/mnorm_${tbl}.txt" | sort -u) \
            <(cut -d'|' -f1 "$TMPD/pnorm_${tbl}.txt" | sort -u) \
      | sed -n '1,40p' | sed 's/^/      /' || true
    fail "Column names differ: ${tbl}"
  else
    [[ $QUIET -eq 0 ]] && echo "  Column names match (${tbl})."
  fi
  # --- Column type mapping (MySQL COLUMN_TYPE -> PostgreSQL udt_name) ---
  my_q "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IFNULL(COLUMN_KEY,'')
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_NAME='${ts}'
        ORDER BY COLUMN_NAME" > "$TMPD/mtype_${tbl}.txt" || true

  pg_q "SELECT column_name, udt_name,
               COALESCE(character_maximum_length::text, '')
        FROM information_schema.columns
        WHERE table_schema='${PG_SCHEMA}' AND table_name='${ts}'
        ORDER BY column_name" > "$TMPD/pgtype_${tbl}.txt" || true

  unset _pg_udt
  declare -A _pg_udt
  while IFS='|' read -r _cn _udt _maxlen; do
    [[ -z "$_cn" ]] && continue
    case "$_udt" in
      varchar|bpchar) [[ -n "$_maxlen" ]] && _pg_udt["$_cn"]="${_udt}(${_maxlen})" || _pg_udt["$_cn"]="$_udt" ;;
      *)              _pg_udt["$_cn"]="$_udt" ;;
    esac
  done < "$TMPD/pgtype_${tbl}.txt"

  if [[ $QUIET -eq 0 ]]; then
    printf '  %-25s %-22s %-22s %-7s %s\n' "COLUMN" "MYSQL_TYPE" "PG_TYPE" "KEY" "MISMATCH"
    printf '  %s\n' "$(printf -- '-%.0s' {1..80})"
  fi
  _type_mismatches=()
  unset _seen_cols; declare -A _seen_cols
  while IFS=$'\t' read -r _cn _dtype _ctype _ckey; do
    [[ -z "$_cn" ]] && continue
    _seen_cols["$_cn"]=1
    _pg_actual="${_pg_udt[$_cn]:-}"
    if [[ -z "$_pg_actual" ]]; then
      printf '  %-25s %-22s %-22s %-7s %s\n' "$_cn" "$_ctype" "---" "$_ckey" "yes"
      _type_mismatches+=("${_cn}: MYSQL_ONLY")
      continue
    fi
    if [[ "$_ctype" == "$_pg_actual" ]]; then
      [[ $QUIET -eq 0 ]] && printf '  %-25s %-22s %-22s %-7s %s\n' "$_cn" "$_ctype" "$_pg_actual" "$_ckey" "no"
    else
      printf '  %-25s %-22s %-22s %-7s %s\n' "$_cn" "$_ctype" "$_pg_actual" "$_ckey" "yes"
      _type_mismatches+=("${_cn}: ${_ctype} -> ${_pg_actual}")
    fi
  done < "$TMPD/mtype_${tbl}.txt"
  # PG-only columns (present in PG but absent in MySQL)
  _pg_only=()
  for _cn in "${!_pg_udt[@]}"; do
    [[ -n "${_seen_cols[$_cn]:-}" ]] || _pg_only+=("$_cn")
  done
  if [[ ${#_pg_only[@]} -gt 0 ]]; then
    while IFS= read -r _cn; do
      printf '  %-25s %-22s %-22s %-7s %s\n' "$_cn" "---" "${_pg_udt[$_cn]}" "" "yes"
      _type_mismatches+=("${_cn}: PG_ONLY")
    done < <(printf '%s\n' "${_pg_only[@]}" | sort)
  fi
  unset _pg_udt _seen_cols _pg_only
  [[ ${#_type_mismatches[@]} -gt 0 ]] && echo "  >>> ${#_type_mismatches[@]} issue(s) above"

  _mysql_idx=$(my_q "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_NAME='${ts}'" || echo "ERR")
  _pg_idx=$(pg_q "SELECT COUNT(*) FROM pg_indexes
    WHERE schemaname='${PG_SCHEMA}' AND tablename='${ts}'" || echo "ERR")
  [[ $QUIET -eq 0 ]] && echo "  Indexes: MySQL=${_mysql_idx}  PG=${_pg_idx}"
  if [[ "$_mysql_idx" != "ERR" && "$_pg_idx" != "ERR" && "$_mysql_idx" != "$_pg_idx" ]]; then
    echo "  >>> Index count mismatch — likely a name collision after 63-char truncation; one index may be missing."
    fail "Index count mismatch: ${tbl} (MySQL=${_mysql_idx} PG=${_pg_idx})"
  fi

  mapfile -t mysql_pk < <(my_q "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
    WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_NAME='${ts}' AND CONSTRAINT_NAME='PRIMARY'
    ORDER BY ORDINAL_POSITION" || true)
  mapfile -t pg_pk < <(pg_q "SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_schema=kcu.constraint_schema AND tc.constraint_name=kcu.constraint_name
    WHERE tc.table_schema='${PG_SCHEMA}' AND tc.table_name='${ts}'
      AND tc.constraint_type='PRIMARY KEY'
    ORDER BY kcu.ordinal_position" || true)

  if [[ "${#mysql_pk[@]}" -eq 1 && "${#pg_pk[@]}" -eq 1 && "${mysql_pk[0]}" == "${pg_pk[0]}" ]]; then
    pk="${mysql_pk[0]}"
    pk_mi=$(mysql_ident "$pk")
    pk_pg=$(pg_ident_col "$pk")
    mi=$(mysql_ident "$tbl")
    pi=$(pg_ident "$PG_SCHEMA" "$tbl")
    if [[ $QUIET -eq 0 ]]; then
      echo ""
      echo "  Primary key (single column, same name): ${pk}"
    fi

    mysql_cnt="${ROWCNT_MYSQL[$tbl]:-ERR}"
    pg_cnt="${ROWCNT_PG[$tbl]:-ERR}"

    if [[ "$mysql_cnt" =~ ^[0-9]+$ && "$pg_cnt" =~ ^[0-9]+$ \
       && "$mysql_cnt" -le "$COMPARE_FINGERPRINT_MAX_ROWS" \
       && "$pg_cnt"    -le "$COMPARE_FINGERPRINT_MAX_ROWS" ]]; then
      _mm=$(my_q "SELECT CONCAT_WS(',',IFNULL(MIN(${pk_mi}),'NULL'),IFNULL(MAX(${pk_mi}),'NULL')) FROM ${mi}" || echo "ERR")
      _pg_mm=$(pg_q "SELECT COALESCE(MIN(${pk_pg})::text,'NULL')||','||COALESCE(MAX(${pk_pg})::text,'NULL') FROM ${pi}" || echo "ERR")
      [[ "$_mm" == "ERR" ]] && mysql_fp="ERR" || mysql_fp="${mysql_cnt},${_mm}"
      [[ "$_pg_mm" == "ERR" ]] && pg_fp="ERR" || pg_fp="${pg_cnt},${_pg_mm}"
      [[ $QUIET -eq 0 ]] && echo "  PK fingerprint COUNT,MIN,MAX: MySQL=${mysql_fp}  PG=${pg_fp}"
      if [[ "$mysql_fp" != "$pg_fp" || "$mysql_fp" == "ERR" || "$pg_fp" == "ERR" ]]; then
        echo "  >>> PK fingerprint mismatch: ${tbl} (MySQL=${mysql_fp}  PG=${pg_fp})"
        fail "PK fingerprint mismatch: ${tbl} (MySQL=${mysql_fp} PG=${pg_fp})"
      fi
    else
      [[ $QUIET -eq 0 ]] && echo "  PK fingerprint skipped (row count not numeric or exceeds COMPARE_FINGERPRINT_MAX_ROWS)."
    fi

    mysql_ai=$(my_q  "SELECT AUTO_INCREMENT FROM information_schema.TABLES
                      WHERE TABLE_SCHEMA='${MYSQL_DB}' AND TABLE_NAME='${ts}'" || true)
    mysql_max=$(my_q "SELECT MAX(${pk_mi}) FROM ${mi}" || echo "ERR")
    pg_max=$(pg_q    "SELECT COALESCE(MAX(${pk_pg})::text,'NULL') FROM ${pi}" || echo "ERR")
    [[ "$mysql_max" == "" ]] && mysql_max="NULL"
    [[ "$pg_max"    == "" ]] && pg_max="NULL"

    seqrel=$(pg_q "SELECT pg_get_serial_sequence('${PG_SCHEMA}.${ts}','${pk}')" || true)
    pg_seq_last=""
    if [[ -n "${seqrel:-}" && "$seqrel" != "ERR" ]]; then
      pg_seq_last=$(pg_q "SELECT last_value FROM \"${seqrel%%.*}\".\"${seqrel#*.}\"" || echo "ERR")
    fi

    if [[ $QUIET -eq 0 ]]; then
      echo "  MySQL AUTO_INCREMENT (next): ${mysql_ai:-NULL}   MAX(${pk}): ${mysql_max}"
      echo "  PG sequence: ${seqrel:-<none>}   last_value: ${pg_seq_last:-n/a}   MAX(${pk}): ${pg_max}"
    fi

    if [[ -n "${mysql_ai:-}" && "${mysql_ai}" != "NULL" && "${mysql_ai}" != "ERR" && -z "${seqrel:-}" ]]; then
      echo "  >>> MySQL column has AUTO_INCREMENT but no sequence exists on PostgreSQL; INSERTs will fail."
      fail "Missing sequence: ${tbl}.${pk} (MySQL AUTO_INCREMENT with no PG sequence/default)"
    fi

    if [[ "$mysql_max" != "$pg_max" ]]; then
      echo "  >>> MAX(pk) differs: ${tbl} (MySQL=${mysql_max} PG=${pg_max})"
      fail "MAX(${pk}) mismatch: ${tbl} (MySQL=${mysql_max} PG=${pg_max})"
    fi
    if [[ -n "${seqrel:-}" && "$seqrel" != "ERR" \
       && "$pg_seq_last" =~ ^-?[0-9]+$ && "$pg_max" =~ ^-?[0-9]+$ ]]; then
      if   [[ "$pg_seq_last" -lt "$pg_max" ]]; then
        echo "  >>> WARNING: sequence last_value (${pg_seq_last}) is behind MAX(pk) (${pg_max}); run setval before INSERTs."
        fail "Sequence behind: ${tbl}.${pk} (last_value=${pg_seq_last} < MAX=${pg_max})"
      elif [[ "$pg_seq_last" -eq "$pg_max" && $QUIET -eq 0 ]]; then
        echo "  NOTE: sequence last_value equals MAX(pk); confirm nextval returns MAX+1 before relying on default inserts."
      fi
    fi
  else
    if [[ $QUIET -eq 0 ]]; then
      echo ""
      echo "  Primary key: MySQL (${#mysql_pk[@]} cols): ${mysql_pk[*]:-—} | PG (${#pg_pk[@]} cols): ${pg_pk[*]:-—}"
      echo "  (Skipping single-column PK fingerprint / sequence check.)"
    fi
  fi

  [[ $QUIET -eq 0 ]] && echo ""
done <"$TMPD/both.txt"

[[ $CHECK_ZERO_DATES -eq 1 ]] && check_zero_dates

echo ""
echo "================================================================"
if [[ "$EXIT" -eq 0 ]]; then
  echo "Summary: ALL CHECKS PASSED — no mismatches found."
else
  echo "Summary: ${#FAILURES[@]} issue(s) found:"
  echo ""
  for i in "${!FAILURES[@]}"; do
    printf '  %2d. %s\n' "$((i+1))" "${FAILURES[$i]}"
  done
fi
echo "================================================================"
echo ""
echo "Full report: ${REPORT}"

# Close stdout/stderr so tee can flush, then wait for it
exec 1>&- 2>&-
wait "$TEE_PID" 2>/dev/null || true

exit "$EXIT"

#!/usr/bin/env python3
"""Compare actual row data between MySQL source and PostgreSQL target.

This script reuses the same environment variables and password conventions as
compare-mysql-pg.sh, then performs per-table content comparison by:
1) finding tables present in both databases,
2) requiring matching primary-key column sets,
3) hashing each row from common columns on each side, and
4) streaming a PK-ordered merge to detect:
   - rows only in MySQL
   - rows only in PostgreSQL
   - rows present on both sides but with different content

It is designed as a data-level complement to schema/count comparison scripts.

Usage
-----
    # Compare all common tables via an env file
    python3 compare-mysql-pg-data.py --env-file migration.env

    # Target a named PostgreSQL schema
    python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras

    # Compare specific tables only
    python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \\
        --table farm,account

    # Show column-level diffs for the first 5 rows of each diff type per table
    python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \\
        --inspect 5

    # Single table with detailed inspection
    python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \\
        --table farm --inspect 10

    # Disable snapshot bounding (compare the full live table, no MAX(pk) cap)
    python3 compare-mysql-pg-data.py --env-file migration.env --no-snapshot-bound

Output
------
Each table row shows:  TABLE  MYSQL_ONLY  PG_ONLY  CHANGED  STATUS

  MYSQL_ONLY  rows present in MySQL but absent from PostgreSQL
  PG_ONLY     rows present in PostgreSQL but absent from MySQL
  CHANGED     rows with a matching PK but at least one column value differs
  STATUS      OK (no differences) or DIFF

When STATUS is DIFF and --inspect N is supplied, the first N rows of each diff
type are fetched and displayed:

  - For CHANGED rows: side-by-side column diff (MySQL value vs PG value).
  - For MYSQL_ONLY and PG_ONLY rows: all column values from that side, plus a
    check of whether the same PK exists on the other side:
      YES (sort-key mismatch)     the row is there; a column-type mismatch
                                  caused the merge not to pair them (e.g. the
                                  id column is text in PG but int in MySQL,
                                  making ORDER BY sort differently)
      NO (row is genuinely absent)  the row is truly missing on the other side

Without --inspect, sample PK values are printed for each diff type so you can
query them directly in your database client.

Since MySQL is the live source and PostgreSQL trails behind via pg_chameleon
replication, comparing "live" data will always show some false differences
from rows changed mid-run. To avoid this, tables with a single-column integer
primary key are automatically bounded to a snapshot: the current MAX(pk) is
captured from MySQL right before that table is compared, both sides are
queried with pk <= that value, and the script pauses briefly (see
--snapshot-lag-seconds) before comparing so replication can catch up. Use
--no-snapshot-bound to disable this and compare the full live table instead.

Reports are written to $LOG_DIR/compare-mysql-pg-data_<timestamp>.txt
(default: ~/migration/logs/).
"""

from __future__ import annotations

import argparse
import os
import queue
import re
import shlex
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

EXCLUDED_TABLES = {"flyway_schema_history", "schema_migrations"}

# Per-table retry policy for transient connection drops (e.g. a long-running
# streaming query getting killed by a network/idle timeout).
TABLE_MAX_ATTEMPTS = 3
TABLE_RETRY_BACKOFF_SECONDS = 10.0

# Substrings (lowercased) of exception messages that indicate a transient
# connection problem worth retrying, rather than a real query/data error.
_TRANSIENT_DB_ERROR_PATTERNS = (
    "lost connection",
    "server has gone away",
    "broken pipe",
    "connection reset",
    "connection refused",
    "could not connect",
    "ssl connection has been closed unexpectedly",
    "terminating connection due to",
    "server closed the connection unexpectedly",
    "timed out",
)


def _is_transient_db_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(pattern in msg for pattern in _TRANSIENT_DB_ERROR_PATTERNS)


# ─── Terminal color output ──────────────────────────────────────────────────
# Colors are only ever applied to what's printed to the console; the log file
# always gets the plain, uncolored text so it stays easy to grep/diff.
_STATUS_COLORS = {
    "OK": "32",  # green
    "DIFF": "33",  # yellow
    "ERROR": "31",  # red
    "SKIP-PK": "36",  # cyan
    "SKIP-COL": "36",  # cyan
}


def _console_supports_color() -> bool:
    return sys.stdout.isatty() and not os.environ.get("NO_COLOR")


def _colorize(text: str, color_code: str) -> str:
    return f"\x1b[{color_code}m{text}\x1b[0m"


# ─── Per-column hash token constants ────────────────────────────────────────
# CHAR(1)/chr(1): NULL sentinel — distinguishes NULL from empty string.
# CHAR(2)/chr(2): opaque-value sentinel for json/jsonb/bytea/bit columns —
#                 only NULL vs non-NULL is reflected in the hash.
# CHAR(28)/chr(28): field separator for multi-column PK concatenation.
# CHAR(29)/chr(29): group separator used inside MD5's CONCAT_WS argument.
_NULL_SENTINEL = "\x01"
_OPAQUE_SENTINEL = "\x02"
_PK_SEP = "\x1c"  # Python-side separator to split multi-col PK values

# MySQL integer types — PK sort keys are zero-padded so string order == numeric order.
_INT_PK_TYPES = frozenset({"tinyint", "smallint", "mediumint", "int", "bigint"})

# PG native integer udt_names — ordering by these columns directly (no CAST) lets
# PostgreSQL use the primary-key index for ORDER BY instead of a full sort. Only
# valid as an ORDER BY fast-path gate on PostgreSQL's own udt_name (pg_udt); the
# MySQL-reported type (pk_types) is not a reliable stand-in since pg_chameleon
# doesn't guarantee a MySQL int column maps to a native PG int column.
_PG_INT_UDTS = frozenset({"int2", "int4", "int8"})

# PG udt_names whose text representations differ from MySQL CAST(... AS CHAR).
# Content is not compared; only NULL vs non-NULL is checked.
_OPAQUE_UDTS = frozenset({"json", "jsonb", "bytea", "bit"})


@dataclass
class Config:
    mysql_host: str
    mysql_user: str
    mysql_db: str
    mysql_password: str
    pg_host: str
    pg_user: str
    pg_db: str
    pg_password: str
    pg_schema: str
    log_dir: Path
    sample_limit: int
    inspect_limit: int
    selected_tables: set[str] | None
    snapshot_bound: bool
    snapshot_lag_seconds: float
    use_color: bool


def mysql_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def pg_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def read_password_file(path: str) -> str:
    p = Path(path).expanduser()
    if not p.is_file():
        raise RuntimeError(f"Cannot read password file: {path}")
    with p.open("r", encoding="utf-8", errors="replace") as f:
        return f.readline().rstrip("\n")


def load_env_file(path: str) -> dict[str, str]:
    env_file = Path(path).expanduser()
    if not env_file.exists():
        raise RuntimeError(f"Env file not found: {path}")

    # Use bash to preserve shell syntax compatibility with existing env files.
    cmd = [
        "bash",
        "-lc",
        f"set -a; source {shlex.quote(str(env_file))}; env -0",
    ]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Failed to source env file {path}: {stderr}")

    data: dict[str, str] = {}
    for item in proc.stdout.split(b"\0"):
        if not item:
            continue
        if b"=" not in item:
            continue
        k, v = item.split(b"=", 1)
        data[k.decode("utf-8", errors="replace")] = v.decode("utf-8", errors="replace")
    return data


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compare MySQL vs PostgreSQL row content",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare all common tables via env file
  python3 compare-mysql-pg-data.py --env-file migration.env

  # Specific tables only
  python3 compare-mysql-pg-data.py --env-file migration.env --table account,license

  # Show per-column diffs for first 5 changed rows per table
  python3 compare-mysql-pg-data.py --env-file migration.env --inspect 5
""",
    )
    ap.add_argument("--env-file", help="Source variables from FILE")
    ap.add_argument("--mysql-host", help="MySQL host (or MYSQL_FQDN)")
    ap.add_argument("--mysql-user", help="MySQL user (or MYSQL_USER)")
    ap.add_argument("--mysql-db", help="MySQL database (or MYSQL_DB)")
    ap.add_argument("--mysql-pass-file", help="Read MySQL password from FILE")
    ap.add_argument("--pg-host", help="PostgreSQL host (or PG_FQDN)")
    ap.add_argument("--pg-user", help="PostgreSQL user (or PG_USER)")
    ap.add_argument("--pg-db", help="PostgreSQL database (or PG_DB)")
    ap.add_argument("--pg-pass-file", help="Read PostgreSQL password from FILE")
    ap.add_argument(
        "--pg-schema", help="PostgreSQL schema (or PG_SCHEMA, default public)"
    )
    ap.add_argument(
        "--table",
        help="Comma-separated table names to compare (default: all common tables)",
    )
    ap.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Max changed-row PK values to retain per table (default: 20)",
    )
    ap.add_argument(
        "--inspect",
        type=int,
        default=0,
        metavar="N",
        help=(
            "After hashing, fetch and display up to N changed rows per table "
            "with per-column diffs (default: 0 = disabled)"
        ),
    )
    ap.add_argument("--log-dir", help="Report output directory (or LOG_DIR)")
    ap.add_argument(
        "--no-snapshot-bound",
        action="store_true",
        help=(
            "Disable automatic snapshot bounding. By default, for tables with a "
            "single-column integer primary key, the current MAX(pk) is captured "
            "from MySQL right before that table's comparison and used as an "
            "upper bound on both sides (WHERE pk <= max), so rows inserted while "
            "the comparison runs are not flagged as false MYSQL_ONLY differences. "
            "Tables with no PK, a composite PK, or a non-integer PK are always "
            "compared unbounded."
        ),
    )
    ap.add_argument(
        "--snapshot-lag-seconds",
        type=float,
        default=5.0,
        metavar="N",
        help=(
            "After capturing the snapshot bound, wait N seconds before comparing "
            "so pg_chameleon replication can catch up on any in-flight changes to "
            "rows at or below the bound, reducing false CHANGED differences "
            "caused by replication lag (default: 5; use 0 to disable the wait)"
        ),
    )
    ap.add_argument(
        "--no-color",
        action="store_true",
        help="Disable colored STATUS output (color is auto-disabled when not a terminal)",
    )
    return ap.parse_args()


def build_config(args: argparse.Namespace) -> Config:
    env: dict[str, str] = dict(os.environ)
    if args.env_file:
        env.update(load_env_file(args.env_file))

    mysql_host = args.mysql_host or env.get("MYSQL_FQDN", "")
    mysql_user = args.mysql_user or env.get("MYSQL_USER", "")
    mysql_db = args.mysql_db or env.get("MYSQL_DB", "")

    pg_host = args.pg_host or env.get("PG_FQDN", "")
    pg_user = args.pg_user or env.get("PG_USER", "")
    pg_db = args.pg_db or env.get("PG_DB", "")
    pg_schema = args.pg_schema or env.get("PG_SCHEMA", "public")

    mysql_password = ""
    pg_password = ""

    if args.mysql_pass_file:
        mysql_password = read_password_file(args.mysql_pass_file)
    elif env.get("MYSQL_PASSWORD"):
        mysql_password = env["MYSQL_PASSWORD"]
    else:
        for p in [Path.home() / ".mysql_migration_pw", Path("/etc/secrets/mysql_pw")]:
            if p.is_file():
                mysql_password = read_password_file(str(p))
                break

    if args.pg_pass_file:
        pg_password = read_password_file(args.pg_pass_file)
    elif env.get("PG_PASSWORD"):
        pg_password = env["PG_PASSWORD"]
    else:
        for p in [Path.home() / ".pg_migration_pw", Path("/etc/secrets/pg_pw")]:
            if p.is_file():
                pg_password = read_password_file(str(p))
                break

    missing = []
    for key, value in [
        ("MYSQL_FQDN", mysql_host),
        ("MYSQL_USER", mysql_user),
        ("MYSQL_DB", mysql_db),
        ("MYSQL_PASSWORD", mysql_password),
        ("PG_FQDN", pg_host),
        ("PG_USER", pg_user),
        ("PG_DB", pg_db),
        ("PG_PASSWORD", pg_password),
    ]:
        if not value:
            missing.append(key)

    if missing:
        raise RuntimeError("Missing required variables: " + " ".join(missing))

    raw = args.table.strip() if args.table else ""
    selected_tables = {t.strip() for t in raw.split(",") if t.strip()} or None

    log_dir = Path(
        args.log_dir or env.get("LOG_DIR") or (Path.home() / "migration/logs")
    )
    inspect_limit = max(0, args.inspect)
    # sample_limit must cover at least inspect_limit rows so there are enough PKs to inspect
    sample_limit = max(max(1, args.sample_limit), inspect_limit)
    snapshot_bound = not args.no_snapshot_bound
    snapshot_lag_seconds = max(0.0, args.snapshot_lag_seconds)
    use_color = _console_supports_color() and not args.no_color

    return Config(
        mysql_host=mysql_host,
        mysql_user=mysql_user,
        mysql_db=mysql_db,
        mysql_password=mysql_password,
        pg_host=pg_host,
        pg_user=pg_user,
        pg_db=pg_db,
        pg_password=pg_password,
        pg_schema=pg_schema,
        log_dir=log_dir,
        sample_limit=sample_limit,
        inspect_limit=inspect_limit,
        selected_tables=selected_tables,
        snapshot_bound=snapshot_bound,
        snapshot_lag_seconds=snapshot_lag_seconds,
        use_color=use_color,
    )


def _mysql_cmd(cfg: Config, sql: str) -> tuple[list[str], dict[str, str]]:
    cmd = [
        "mysql",
        "-h",
        cfg.mysql_host,
        "-u",
        cfg.mysql_user,
        "--ssl-mode=REQUIRED",
        "-D",
        cfg.mysql_db,
        "-N",
        "-B",
        # --quick uses mysql_use_result() so rows stream to stdout as the server
        # sends them, instead of the client buffering the entire result set in
        # memory before printing anything (mysql_store_result(), the default).
        # This matters a lot on large tables.
        "--quick",
        "-e",
        sql,
    ]
    return cmd, {**os.environ, "MYSQL_PWD": cfg.mysql_password}


def _pg_cmd(cfg: Config, sql: str) -> tuple[list[str], dict[str, str]]:
    cmd = [
        "psql",
        "-h",
        cfg.pg_host,
        "-U",
        cfg.pg_user,
        "-d",
        cfg.pg_db,
        "--no-password",
        "-At",
        "-F",
        "\t",
        "-c",
        sql,
    ]
    return cmd, {**os.environ, "PGPASSWORD": cfg.pg_password, "PGSSLMODE": "require"}


def run_mysql(cfg: Config, sql: str) -> str:
    cmd, env = _mysql_cmd(cfg, sql)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "MySQL query failed")
    return proc.stdout


def run_pg(cfg: Config, sql: str) -> str:
    cmd, env = _pg_cmd(cfg, sql)
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "PostgreSQL query failed")
    return proc.stdout


def _pg_copy_cmd(cfg: Config, select_sql: str) -> tuple[list[str], dict[str, str]]:
    """Wrap a SELECT in COPY ... TO STDOUT so psql streams rows via the COPY
    protocol (PQgetCopyData) instead of buffering the whole result set in
    client memory first, as plain `psql -c "SELECT ..."` does. This matters a
    lot on large tables. COPY text format also backslash-escapes any embedded
    tabs/newlines in field values, so splitting on a literal tab is safe.
    """
    copy_sql = f"COPY ({select_sql}) TO STDOUT WITH (FORMAT text)"
    cmd = [
        "psql",
        "-h",
        cfg.pg_host,
        "-U",
        cfg.pg_user,
        "-d",
        cfg.pg_db,
        "--no-password",
        "-c",
        copy_sql,
    ]
    return cmd, {**os.environ, "PGPASSWORD": cfg.pg_password, "PGSSLMODE": "require"}


def _stream_rows(
    cmd: list[str], env: dict[str, str], label: str
) -> Iterator[tuple[str, str, str]]:
    """Stream (pk_key, pk_val, row_hash) tuples from a CLI database process.

    Reading happens on a background thread into a bounded queue, so the OS
    pipe is drained continuously regardless of how fast the merge consumes
    rows. Without this, merge_compare's sorted-merge only pulls from whichever
    side needs to advance; during a long one-sided run (e.g. a large block of
    rows only present on one side) the *other* side's iterator is never
    touched, its subprocess's stdout pipe fills up, the database blocks trying
    to send more rows, and the connection is eventually dropped by a network
    or idle timeout (seen in practice as "Lost connection to MySQL server
    during query"). Buffering ahead in a queue gives much more slack before
    that backpressure can reach the database connection.
    """
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    assert proc.stdout is not None
    assert proc.stderr is not None

    q: "queue.Queue[object]" = queue.Queue(maxsize=50_000)
    _sentinel = object()

    def _reader() -> None:
        try:
            for line in proc.stdout:
                line = line.rstrip("\n")
                if line:
                    q.put(line)
        finally:
            q.put(_sentinel)

    reader_thread = threading.Thread(target=_reader, daemon=True)
    reader_thread.start()

    try:
        while True:
            item = q.get()
            if item is _sentinel:
                break
            parts = item.split("\t", 2)
            if len(parts) != 3:
                continue
            yield parts[0], parts[1], parts[2]
    finally:
        reader_thread.join()
        rc = proc.wait()
        if rc != 0:
            err = proc.stderr.read().strip()
            raise RuntimeError(err or f"{label} streaming query failed")


def iter_mysql(cfg: Config, sql: str) -> Iterator[tuple[str, str, str]]:
    """Stream (pk_key, pk_val, row_hash) rows from MySQL."""
    cmd, env = _mysql_cmd(cfg, sql)
    return _stream_rows(cmd, env, "MySQL")


def iter_pg(cfg: Config, sql: str) -> Iterator[tuple[str, str, str]]:
    """Stream (pk_key, pk_val, row_hash) rows from PostgreSQL."""
    cmd, env = _pg_copy_cmd(cfg, sql)
    return _stream_rows(cmd, env, "PostgreSQL")


def parse_lines(raw: str) -> list[str]:
    return [ln.strip() for ln in raw.splitlines() if ln.strip()]


def get_mysql_tables(cfg: Config) -> set[str]:
    sql = (
        "SELECT TABLE_NAME FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} AND TABLE_TYPE='BASE TABLE' ORDER BY 1"
    )
    return set(parse_lines(run_mysql(cfg, sql)))


def get_pg_tables(cfg: Config) -> set[str]:
    sql = (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema={sql_literal(cfg.pg_schema)} AND table_type='BASE TABLE' ORDER BY 1"
    )
    return set(parse_lines(run_pg(cfg, sql)))


def get_mysql_pk(cfg: Config, table: str) -> list[str]:
    sql = (
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} "
        f"AND TABLE_NAME={sql_literal(table)} "
        "AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION"
    )
    return parse_lines(run_mysql(cfg, sql))


def get_pg_pk(cfg: Config, table: str) -> list[str]:
    sql = (
        "SELECT kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_schema=kcu.constraint_schema "
        "AND tc.constraint_name=kcu.constraint_name "
        f"WHERE tc.table_schema={sql_literal(cfg.pg_schema)} "
        f"AND tc.table_name={sql_literal(table)} "
        "AND tc.constraint_type='PRIMARY KEY' "
        "ORDER BY kcu.ordinal_position"
    )
    return parse_lines(run_pg(cfg, sql))


def get_mysql_columns(cfg: Config, table: str) -> list[str]:
    sql = (
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} "
        f"AND TABLE_NAME={sql_literal(table)} "
        "ORDER BY ORDINAL_POSITION"
    )
    return parse_lines(run_mysql(cfg, sql))


def get_pg_udt_map(cfg: Config, table: str) -> dict[str, str]:
    sql = (
        "SELECT column_name, udt_name FROM information_schema.columns "
        f"WHERE table_schema={sql_literal(cfg.pg_schema)} "
        f"AND table_name={sql_literal(table)}"
    )
    out = run_pg(cfg, sql)
    udt: dict[str, str] = {}
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t", 1)
        if len(parts) == 2:
            udt[parts[0]] = parts[1]
    return udt


def get_mysql_max_pk(cfg: Config, table: str, pk_col: str) -> str | None:
    """Return the current MAX(pk_col) from MySQL as a decimal string, or None if
    the table is empty. Used as a snapshot upper bound so rows inserted after
    the comparison starts aren't flagged as false MYSQL_ONLY differences.
    """
    sql = f"SELECT MAX({mysql_ident(pk_col)}) FROM {mysql_ident(table)}"
    out = run_mysql(cfg, sql).strip()
    return out or None


def get_pk_data_types(cfg: Config, table: str) -> dict[str, str]:
    """Return {column_name: mysql_data_type} for all primary-key columns."""
    sql = (
        "SELECT c.COLUMN_NAME, c.DATA_TYPE "
        "FROM information_schema.KEY_COLUMN_USAGE k "
        "JOIN information_schema.COLUMNS c "
        "  ON k.TABLE_SCHEMA = c.TABLE_SCHEMA "
        "  AND k.TABLE_NAME  = c.TABLE_NAME "
        "  AND k.COLUMN_NAME = c.COLUMN_NAME "
        f"WHERE k.TABLE_SCHEMA = {sql_literal(cfg.mysql_db)} "
        f"  AND k.TABLE_NAME   = {sql_literal(table)} "
        "  AND k.CONSTRAINT_NAME = 'PRIMARY' "
        "ORDER BY k.ORDINAL_POSITION"
    )
    out = run_mysql(cfg, sql)
    result: dict[str, str] = {}
    for line in parse_lines(out):
        parts = line.split("\t", 1)
        if len(parts) == 2:
            result[parts[0]] = parts[1].lower()
    return result


# ─── Column token expressions ────────────────────────────────────────────
# Each expression returns a NOT-NULL string that is byte-identical on both
# databases when the underlying data is the same value.  Key differences
# handled here:
#
#   bool         MySQL 0/1  vs  PG true/false   → both emit 'true'/'false'
#   datetime → timestamptz  MySQL 'YYYY-MM-DD HH:MM:SS'  vs
#                             PG 'YYYY-MM-DD HH:MM:SS+00'  → normalised to
#                             'YYYY-MM-DD HH24:MI:SS' (no tz, no sub-second)
#   json/jsonb/bytea/bit     text representation differs → only NULL≠non-NULL
#   all others               direct CAST(… AS CHAR) / ::text
#
# The old approach (HEX-encoding every value then MD5-ing) produced different
# hashes for identical data because MySQL HEX() returns uppercase while
# PostgreSQL encode(…,'hex') returns lowercase.


def mysql_col_token(col: str, pg_udt: str) -> str:
    ci = mysql_ident(col)
    if pg_udt in _OPAQUE_UDTS:
        return f"CASE WHEN {ci} IS NULL THEN CHAR(1) ELSE CHAR(2) END"
    if pg_udt == "bool":
        return (
            f"CASE WHEN {ci} IS NULL THEN CHAR(1) "
            f"WHEN CAST({ci} AS UNSIGNED) = 0 THEN 'false' ELSE 'true' END"
        )
    if pg_udt in ("timestamptz", "timestamp"):
        # DATE_FORMAT produces 'YYYY-MM-DD HH:MM:SS' — no timezone, no sub-second
        return (
            f"CASE WHEN {ci} IS NULL THEN CHAR(1) "
            f"ELSE DATE_FORMAT({ci}, '%Y-%m-%d %H:%i:%s') END"
        )
    return f"COALESCE(CAST({ci} AS CHAR), CHAR(1))"


def pg_col_token(col: str, pg_udt: str) -> str:
    ci = pg_ident(col)
    if pg_udt in _OPAQUE_UDTS:
        return f"CASE WHEN {ci} IS NULL THEN chr(1) ELSE chr(2) END"
    if pg_udt == "bool":
        return (
            f"CASE WHEN {ci} IS NULL THEN chr(1) "
            f"WHEN {ci} THEN 'true' ELSE 'false' END"
        )
    if pg_udt == "timestamptz":
        # Strip timezone and sub-second to match MySQL CAST(datetime AS CHAR)
        return (
            f"CASE WHEN {ci} IS NULL THEN chr(1) "
            f"ELSE to_char({ci} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') END"
        )
    if pg_udt == "timestamp":
        return (
            f"CASE WHEN {ci} IS NULL THEN chr(1) "
            f"ELSE to_char({ci}, 'YYYY-MM-DD HH24:MI:SS') END"
        )
    return f"COALESCE({ci}::text, chr(1))"


# ─── PK sort-key and raw-value expressions ────────────────────────────────────
# pk_key  – zero-padded for integer PKs so string sort order == numeric sort order
# pk_val  – raw text representation used verbatim in WHERE clauses for row inspection


def _mysql_pk_key(col: str, dtype: str) -> str:
    ci = mysql_ident(col)
    return (
        f"LPAD(CAST({ci} AS CHAR), 20, '0')"
        if dtype in _INT_PK_TYPES
        else f"CAST({ci} AS CHAR)"
    )


def _pg_pk_key(col: str, dtype: str) -> str:
    ci = pg_ident(col)
    expr = f"LPAD({ci}::text, 20, '0')" if dtype in _INT_PK_TYPES else f"{ci}::text"
    # Force byte-wise "C" collation explicitly: PostgreSQL's default database
    # collation (locale-aware, e.g. en_US.utf8/ICU) is not guaranteed to sort a
    # text value the same way as Python's plain string comparison in
    # merge_compare. Pinning "C" here guarantees the two always agree.
    return f'({expr}) COLLATE "C"'


def _mysql_pk_val(col: str) -> str:
    return f"CAST({mysql_ident(col)} AS CHAR)"


def _pg_pk_val(col: str) -> str:
    return f"{pg_ident(col)}::text"


# ─── Hash queries ──────────────────────────────────────────────────────
# Each query yields three tab-separated columns per row:
#   pk_key   – sort key for the merge (zero-padded integers sort correctly)
#   pk_val   – real PK value(s) for use in inspection WHERE clauses
#   row_hash – MD5 of CONCAT_WS(GS, col_token, …) over all common columns


def build_mysql_hash_query(
    cfg: Config,
    table: str,
    pk: list[str],
    pk_types: dict[str, str],
    cols: list[str],
    pg_udt: dict[str, str],
    max_pk: str | None = None,
) -> str:
    key_exprs = [_mysql_pk_key(c, pk_types.get(c, "")) for c in pk]
    val_exprs = [_mysql_pk_val(c) for c in pk]
    tok_exprs = [mysql_col_token(c, pg_udt.get(c, "")) for c in cols]
    # Fast path: for a single-column non-negative integer PK, ordering by the
    # bare column lets MySQL use the PK index instead of a full filesort over
    # the computed pk_key expression -- for large tables the filesort was
    # costly enough to cause hour-plus runtimes and connection drops. This is
    # safe by construction (not an equivalence guess): ascending native integer
    # order is identical to ascending order of the same integers zero-padded to
    # a fixed width, regardless of collation. Composite/non-integer PKs fall
    # back to ORDER BY 1 (the pk_key expression itself), which merge_compare's
    # order_warning diagnostic will flag if it's ever violated.
    if len(pk) == 1 and pk_types.get(pk[0], "") in _INT_PK_TYPES:
        order_by = mysql_ident(pk[0])
    else:
        order_by = "1"
    ti = mysql_ident(table)

    pk_key_sql = (
        key_exprs[0]
        if len(key_exprs) == 1
        else "CONCAT(" + ", CHAR(28), ".join(key_exprs) + ")"
    )
    pk_val_sql = (
        val_exprs[0]
        if len(val_exprs) == 1
        else "CONCAT(" + ", CHAR(28), ".join(val_exprs) + ")"
    )
    row_hash = f"MD5(CONCAT_WS(CHAR(29), {', '.join(tok_exprs)}))"
    # max_pk bounds the comparison to a snapshot (pk[0] <= max_pk) so rows
    # inserted while the comparison is running don't show up as false diffs.
    # Only used for single-column integer PKs; max_pk is pre-validated as
    # matching ^-?\d+$ before it reaches here, so it's safe to interpolate.
    where_sql = f" WHERE {mysql_ident(pk[0])} <= {max_pk}" if max_pk is not None else ""

    return f"SELECT {pk_key_sql}, {pk_val_sql}, {row_hash} FROM {ti}{where_sql} ORDER BY {order_by}"


def build_pg_hash_query(
    cfg: Config,
    table: str,
    pk: list[str],
    pk_types: dict[str, str],
    cols: list[str],
    pg_udt: dict[str, str],
    max_pk: str | None = None,
) -> str:
    key_exprs = [_pg_pk_key(c, pk_types.get(c, "")) for c in pk]
    val_exprs = [_pg_pk_val(c) for c in pk]
    tok_exprs = [pg_col_token(c, pg_udt.get(c, "")) for c in cols]
    # Fast path: for a single-column PK whose PostgreSQL column is itself a
    # native integer type, ordering by the bare column lets PostgreSQL use the
    # PK index instead of a full sort over the computed pk_key expression --
    # ORDER BY 1 unconditionally made every table scan pay for a full sort,
    # which for large tables caused hour-plus runtimes. This is safe by
    # construction (ascending native integer order == ascending zero-padded
    # text order, regardless of collation) -- but it MUST be gated on the
    # actual PostgreSQL udt_name (pg_udt), not the MySQL-reported type
    # (pk_types): pg_chameleon doesn't guarantee a MySQL int column lands on a
    # native PG int column (e.g. it can become numeric/text/varchar instead),
    # and gating on the wrong side's type here is exactly what reintroduced
    # the auth_permission sort-order bug (COLLATE "C" only fixes text
    # collation, not a plain lexicographic-vs-numeric ordering mismatch).
    # Composite or non-native-int PKs fall back to ORDER BY 1, which
    # merge_compare's order_warning diagnostic will flag if this is ever
    # violated again.
    if len(pk) == 1 and pg_udt.get(pk[0], "") in _PG_INT_UDTS:
        order_by = pg_ident(pk[0])
    else:
        order_by = "1"
    ti = f"{pg_ident(cfg.pg_schema)}.{pg_ident(table)}"

    _sep = " || chr(28) || "
    pk_key_sql = key_exprs[0] if len(key_exprs) == 1 else _sep.join(key_exprs)
    pk_val_sql = val_exprs[0] if len(val_exprs) == 1 else _sep.join(val_exprs)
    row_hash = f"md5(concat_ws(chr(29), {', '.join(tok_exprs)}))"
    # Same snapshot bound as the MySQL side (see build_mysql_hash_query), applied
    # to the same pk[0] column/value so both sides read an identical row set.
    where_sql = f" WHERE {pg_ident(pk[0])} <= {max_pk}" if max_pk is not None else ""

    return f"SELECT {pk_key_sql}, {pk_val_sql}, {row_hash} FROM {ti}{where_sql} ORDER BY {order_by}"


def next_or_none(it: Iterator[tuple[str, str, str]]) -> tuple[str, str, str] | None:
    try:
        return next(it)
    except StopIteration:
        return None


def merge_compare(
    left: Iterator[tuple[str, str, str]],
    right: Iterator[tuple[str, str, str]],
    sample_limit: int,
) -> tuple[int, int, int, list[str], list[str], list[str], str | None]:
    """Merge two pk_key-ordered streams.

    Each stream yields (pk_key, pk_val, row_hash).
    Returns (mysql_only, pg_only, changed, changed_pk_vals, mysql_only_pks,
    pg_only_pks, order_warning).

    *_pks lists hold actual pk_val strings (not zero-padded sort keys) for up to
    sample_limit rows of each diff type, usable directly in WHERE clauses.

    order_warning is set (once) if either stream is ever observed going
    backwards in pk_key order. The merge assumes both sides arrive in
    ascending pk_key order; if that assumption doesn't hold, every row after
    the inversion point can misclassify as a false MYSQL_ONLY/PG_ONLY
    difference, so callers should treat the counts as unreliable (not a real
    data difference) whenever this is set.
    """
    mysql_only = 0
    pg_only = 0
    changed = 0
    changed_pks: list[str] = []
    mysql_only_pks: list[str] = []
    pg_only_pks: list[str] = []
    order_warning: str | None = None
    last_lk: str | None = None
    last_rk: str | None = None

    def _check_order(side: str, key: str, last: str | None) -> None:
        nonlocal order_warning
        if order_warning is None and last is not None and key < last:
            order_warning = (
                f"{side} query did not return rows in ascending PK order "
                f"({key!r} arrived after {last!r}). merge_compare assumes ascending "
                "order, so the MYSQL_ONLY/PG_ONLY counts below are a sort-order "
                "artifact, not a real data difference -- check the PK column's "
                "actual data type on both sides."
            )

    l = next_or_none(left)
    r = next_or_none(right)

    while l is not None or r is not None:
        if l is None:
            rk, rv, _ = r  # type: ignore[misc]
            _check_order("PostgreSQL", rk, last_rk)
            last_rk = rk
            pg_only += 1
            if len(pg_only_pks) < sample_limit:
                pg_only_pks.append(rv)
            r = next_or_none(right)
            continue
        if r is None:
            lk, lv, _ = l
            _check_order("MySQL", lk, last_lk)
            last_lk = lk
            mysql_only += 1
            if len(mysql_only_pks) < sample_limit:
                mysql_only_pks.append(lv)
            l = next_or_none(left)
            continue

        lk, lv, lh = l
        rk, rv, rh = r
        _check_order("MySQL", lk, last_lk)
        _check_order("PostgreSQL", rk, last_rk)
        last_lk = lk
        last_rk = rk

        if lk == rk:
            if lh != rh:
                changed += 1
                if len(changed_pks) < sample_limit:
                    changed_pks.append(lv)
            l = next_or_none(left)
            r = next_or_none(right)
        elif lk < rk:
            mysql_only += 1
            if len(mysql_only_pks) < sample_limit:
                mysql_only_pks.append(lv)
            l = next_or_none(left)
        else:
            pg_only += 1
            if len(pg_only_pks) < sample_limit:
                pg_only_pks.append(rv)
            r = next_or_none(right)

    return (
        mysql_only,
        pg_only,
        changed,
        changed_pks,
        mysql_only_pks,
        pg_only_pks,
        order_warning,
    )


def dict_compare(
    left: Iterator[tuple[str, str, str]],
    right: Iterator[tuple[str, str, str]],
    sample_limit: int,
) -> tuple[int, int, int, list[str], list[str], list[str]]:
    """Order-independent comparison: buffers both streams fully into dicts
    keyed by pk_key, then computes set differences. Correct regardless of the
    physical delivery order of either stream (unlike merge_compare's sorted
    merge), at the cost of O(n) memory for both sides instead of O(1).

    Used as an automatic fallback for a single table when merge_compare
    reports order_warning -- i.e. the DB didn't return rows in the ascending
    pk_key order the sorted merge requires. Whatever the reason (planner
    choice, replica quirks, etc.), this fallback sidesteps the assumption
    entirely rather than depending on further ORDER BY tuning.
    """
    left_map: dict[str, tuple[str, str]] = {}
    for lk, lv, lh in left:
        left_map[lk] = (lv, lh)
    right_map: dict[str, tuple[str, str]] = {}
    for rk, rv, rh in right:
        right_map[rk] = (rv, rh)

    mysql_only_keys = sorted(k for k in left_map if k not in right_map)
    pg_only_keys = sorted(k for k in right_map if k not in left_map)
    common_keys = left_map.keys() & right_map.keys()
    changed_keys = sorted(k for k in common_keys if left_map[k][1] != right_map[k][1])

    changed_pks = [left_map[k][0] for k in changed_keys[:sample_limit]]
    mysql_only_pks = [left_map[k][0] for k in mysql_only_keys[:sample_limit]]
    pg_only_pks = [right_map[k][0] for k in pg_only_keys[:sample_limit]]

    return (
        len(mysql_only_keys),
        len(pg_only_keys),
        len(changed_keys),
        changed_pks,
        mysql_only_pks,
        pg_only_pks,
    )


# ─── Row inspection ───────────────────────────────────────────────────────


def _py_normalize(val: str, pg_udt: str, side: str) -> str:
    """Mirror the SQL token normalisation in Python for column-level diff comparison."""
    if pg_udt in _OPAQUE_UDTS:
        return _OPAQUE_SENTINEL if val else _NULL_SENTINEL
    if pg_udt == "bool":
        if not val:
            return _NULL_SENTINEL
        if side == "mysql":
            try:
                return "false" if int(val) == 0 else "true"
            except ValueError:
                return val.lower()
        # PostgreSQL returns 't'/'f' in unaligned mode
        return "true" if val.lower() in ("t", "true", "1", "yes", "on") else "false"
    if pg_udt in ("timestamptz", "timestamp"):
        if not val:
            return _NULL_SENTINEL
        # Strip sub-second and timezone: '2024-01-15 10:30:00.123456+00' → '2024-01-15 10:30:00'
        v = val.split(".")[0]
        for sep in ("+", "-"):
            if sep in v[10:]:  # avoid splitting the date portion
                v = v[: v.index(sep, 10)]
        return v.strip()
    return val if val else _NULL_SENTINEL


def _pk_where(pk_cols: list[str], pk_val: str, side: str) -> str:
    """Build a WHERE clause matching a composite PK on the given side (mysql/pg)."""
    ident = mysql_ident if side == "mysql" else pg_ident
    return " AND ".join(
        f"{ident(c)} = {sql_literal(v)}" for c, v in zip(pk_cols, pk_val.split(_PK_SEP))
    )


def _pk_display(pk_cols: list[str], pk_val: str) -> str:
    """Format a PK value for human-readable display."""
    if len(pk_cols) == 1:
        return f"{pk_cols[0]}={pk_val}"
    return "  ".join(f"{c}={v}" for c, v in zip(pk_cols, pk_val.split(_PK_SEP)))


def inspect_changed_rows(
    cfg: Config,
    table: str,
    pk_cols: list[str],
    all_cols: list[str],
    pg_udt: dict[str, str],
    changed_pk_vals: list[str],
    inspect_limit: int,
    out_file: Path,
) -> None:
    """Fetch changed rows from both databases and display per-column differences."""
    sample = changed_pk_vals[:inspect_limit]
    ti_m = mysql_ident(table)
    ti_p = f"{pg_ident(cfg.pg_schema)}.{pg_ident(table)}"
    cols_m = ", ".join(mysql_ident(c) for c in all_cols)
    cols_p = ", ".join(pg_ident(c) for c in all_cols)
    col_w, val_w = 36, 45
    output: list[str] = [
        f"  --- Row inspection: first {len(sample)} changed row(s) ---"
    ]

    for pk_val in sample:
        output.append(f"  ── PK: {_pk_display(pk_cols, pk_val)}")
        output.append(
            f"  {'Column':{col_w}}  {'MySQL':{val_w}}  {'PostgreSQL':{val_w}}  Status"
        )
        output.append(f"  {'-'*col_w}  {'-'*val_w}  {'-'*val_w}  ------")

        try:
            mysql_raw = run_mysql(
                cfg,
                f"SELECT {cols_m} FROM {ti_m} WHERE {_pk_where(pk_cols, pk_val, 'mysql')} LIMIT 1",
            )
            pg_raw = run_pg(
                cfg,
                f"SELECT {cols_p} FROM {ti_p} WHERE {_pk_where(pk_cols, pk_val, 'pg')} LIMIT 1",
            )
        except Exception as exc:
            output.append(f"    fetch error: {exc}")
            continue

        # Use rstrip("\n") not strip() — strip() eats trailing tabs when the last
        # column is NULL, making psql's empty-string NULL appear as a missing field.
        mysql_parts = mysql_raw.strip().split("\t") if mysql_raw.strip() else []
        pg_line = pg_raw.rstrip("\n")
        pg_parts = pg_line.split("\t") if pg_line else []

        if len(mysql_parts) != len(all_cols):
            output.append(
                f"    MySQL: row not found or column count mismatch ({len(mysql_parts)} vs {len(all_cols)})"
            )
            continue
        if len(pg_parts) != len(all_cols):
            output.append(
                f"    PG: row not found or column count mismatch ({len(pg_parts)} vs {len(all_cols)})"
            )
            continue

        mysql_row = dict(zip(all_cols, mysql_parts))
        pg_row = dict(zip(all_cols, pg_parts))
        any_diff = False

        for col in all_cols:
            udt = pg_udt.get(col, "")
            mv = mysql_row.get(col, "")
            pv = pg_row.get(col, "")
            if _py_normalize(mv, udt, "mysql") == _py_normalize(pv, udt, "pg"):
                continue
            any_diff = True
            note = " [opaque — null vs non-null only]" if udt in _OPAQUE_UDTS else ""
            mv_d = (mv[: val_w - 1] + "…") if len(mv) > val_w else (mv or "NULL")
            pv_d = (pv[: val_w - 1] + "…") if len(pv) > val_w else (pv or "NULL")
            output.append(
                f"  {col:{col_w}}  {mv_d:{val_w}}  {pv_d:{val_w}}  DIFF{note}"
            )

        if not any_diff:
            output.append(
                "    (hash mismatch but all columns matched after normalisation — "
                "type may need coverage; please report)"
            )
        output.append("")

    print_and_write(output, out_file)


def inspect_one_sided_rows(
    cfg: Config,
    table: str,
    pk_cols: list[str],
    all_cols: list[str],
    side: str,  # "mysql" or "pg"
    pk_vals: list[str],
    inspect_limit: int,
    out_file: Path,
) -> None:
    """Show actual column values for rows that exist on only one side."""
    sample = pk_vals[:inspect_limit]
    side_label = "MySQL" if side == "mysql" else "PostgreSQL"
    ti_m = mysql_ident(table)
    ti_p = f"{pg_ident(cfg.pg_schema)}.{pg_ident(table)}"
    cols_m = ", ".join(mysql_ident(c) for c in all_cols)
    cols_p = ", ".join(pg_ident(c) for c in all_cols)
    col_w, val_w = 36, 55
    output: list[str] = [
        f"  --- {side_label}-only rows: first {len(sample)} sample(s) ---"
    ]

    for pk_val in sample:
        output.append(f"  ── {side_label} PK: {_pk_display(pk_cols, pk_val)}")
        output.append(
            f"  {'Column':{col_w}}  {side_label+' value':{val_w}}  Also in other DB?"
        )
        output.append(f"  {'-'*col_w}  {'-'*val_w}  -----------------")

        try:
            if side == "mysql":
                raw = run_mysql(
                    cfg,
                    f"SELECT {cols_m} FROM {ti_m} WHERE {_pk_where(pk_cols, pk_val, 'mysql')} LIMIT 1",
                )
            else:
                raw = run_pg(
                    cfg,
                    f"SELECT {cols_p} FROM {ti_p} WHERE {_pk_where(pk_cols, pk_val, 'pg')} LIMIT 1",
                )
        except Exception as exc:
            output.append(f"    fetch error: {exc}")
            continue

        # Use rstrip("\n") not strip() — strip() eats the trailing tab produced by
        # psql when the last column is NULL, making it look like a missing field.
        row_line = raw.rstrip("\n") if side == "pg" else raw.strip()
        parts = row_line.split("\t") if row_line else []
        if len(parts) != len(all_cols):
            output.append(
                f"    row not found or column count mismatch ({len(parts)} vs {len(all_cols)})"
            )
            continue

        # Check whether the other side also has this PK
        try:
            if side == "mysql":
                other_raw = run_pg(
                    cfg,
                    f"SELECT {cols_p} FROM {ti_p} WHERE {_pk_where(pk_cols, pk_val, 'pg')} LIMIT 1",
                )
                other_exists = bool(other_raw.rstrip("\n"))
            else:
                other_raw = run_mysql(
                    cfg,
                    f"SELECT {cols_m} FROM {ti_m} WHERE {_pk_where(pk_cols, pk_val, 'mysql')} LIMIT 1",
                )
                other_exists = bool(other_raw.strip())
        except Exception:
            other_exists = False

        other_note = (
            "YES (sort-key mismatch)"
            if other_exists
            else "NO (row is genuinely absent)"
        )
        for col, val in zip(all_cols, parts):
            val_d = (val[: val_w - 1] + "…") if len(val) > val_w else (val or "NULL")
            output.append(f"  {col:{col_w}}  {val_d:{val_w}}")
        output.append(f"  {'':>{col_w}}  {'':>{val_w}}  → {other_note}")
        output.append("")

    print_and_write(output, out_file)


def print_and_write(lines: Iterable[str], out_file: Path) -> None:
    with out_file.open("a", encoding="utf-8") as f:
        for line in lines:
            print(line)
            f.write(line + "\n")


def print_status_line(
    cfg: Config,
    table: str,
    mysql_only: str,
    pg_only: str,
    changed: str,
    status: str,
    out_file: Path,
) -> None:
    """Print/write a TABLE MYSQL_ONLY PG_ONLY CHANGED STATUS row.

    The log file always gets the plain row; the console gets the STATUS field
    colorized (when supported) so OK/DIFF/ERROR/SKIP rows are easy to scan at
    a glance in a long run.
    """
    plain = f"{table:40} {mysql_only:>12} {pg_only:>12} {changed:>12} {status:>8}"
    with out_file.open("a", encoding="utf-8") as f:
        f.write(plain + "\n")
    if cfg.use_color and status in _STATUS_COLORS:
        colored_status = _colorize(f"{status:>8}", _STATUS_COLORS[status])
        print(
            f"{table:40} {mysql_only:>12} {pg_only:>12} {changed:>12} {colored_status}"
        )
    else:
        print(plain)


def main() -> int:
    args = parse_args()
    try:
        cfg = build_config(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    start_time = datetime.now(timezone.utc)
    ts = start_time.strftime("%Y%m%d_%H%M%S")
    report = cfg.log_dir / f"compare-mysql-pg-data_{ts}.txt"

    print_and_write(
        [
            "================================================================",
            f" MySQL vs PostgreSQL data comparison — {datetime.now(timezone.utc)}",
            f" MySQL:      {cfg.mysql_host} / {cfg.mysql_db}",
            f" PostgreSQL: {cfg.pg_host} / {cfg.pg_db} / schema {cfg.pg_schema}",
            f" Report:     {report}",
            "================================================================",
            "",
        ],
        report,
    )

    try:
        mysql_tables = get_mysql_tables(cfg)
        pg_tables = get_pg_tables(cfg)
    except Exception as exc:
        print_and_write([f"ERROR: failed to load table lists: {exc}"], report)
        return 1

    both = sorted((mysql_tables & pg_tables) - EXCLUDED_TABLES)
    if cfg.selected_tables is not None:
        both = [t for t in both if t in cfg.selected_tables]

    if not both:
        print_and_write(["No common tables to compare."], report)
        return 0

    print_and_write(
        [
            f"Tables: {len(both)} in common",
            "",
            f"{'TABLE':40} {'MYSQL_ONLY':>12} {'PG_ONLY':>12} {'CHANGED':>12} {'STATUS':>8}",
            "-" * 88,
        ],
        report,
    )

    failures: list[tuple[str, int, int, int]] = (
        []
    )  # (table, mysql_only, pg_only, changed)
    skipped: list[str] = []

    for table in both:
        try:
            mysql_pk = get_mysql_pk(cfg, table)
            pg_pk = get_pg_pk(cfg, table)

            if not mysql_pk or not pg_pk:
                skipped.append(f"{table}: no primary key on one or both sides")
                print_status_line(cfg, table, "-", "-", "-", "SKIP-PK", report)
                continue
            if mysql_pk != pg_pk:
                skipped.append(
                    f"{table}: PK mismatch  MySQL=[{','.join(mysql_pk)}]  PG=[{','.join(pg_pk)}]"
                )
                print_status_line(cfg, table, "-", "-", "-", "SKIP-PK", report)
                continue

            pk_types = get_pk_data_types(cfg, table)
            mysql_cols = get_mysql_columns(cfg, table)
            pg_udt = get_pg_udt_map(cfg, table)
            common_cols = [c for c in mysql_cols if c in pg_udt]

            if not common_cols:
                skipped.append(f"{table}: no common columns")
                print_status_line(cfg, table, "-", "-", "-", "SKIP-COL", report)
                continue

            # Snapshot bound: cap the comparison at the current MAX(pk) from MySQL
            # so rows inserted while the comparison runs aren't flagged as false
            # MYSQL_ONLY differences. Only meaningful for a single-column integer
            # PK (MAX() has no useful ordering interpretation for composite or
            # non-integer keys). After capturing the bound, pause briefly to give
            # pg_chameleon replication a chance to catch up on rows at or below
            # it, reducing false CHANGED differences caused by replication lag.
            # The bound message itself is only printed later if the table turns
            # out to be a DIFF — for the common case of a clean OK result it would
            # just be noise cluttering the output.
            max_pk: str | None = None
            snapshot_note: str | None = None
            if (
                cfg.snapshot_bound
                and len(mysql_pk) == 1
                and pk_types.get(mysql_pk[0], "") in _INT_PK_TYPES
            ):
                try:
                    candidate = get_mysql_max_pk(cfg, table, mysql_pk[0])
                except Exception as exc:
                    print_and_write(
                        [f"  WARNING: could not capture snapshot bound: {exc}"],
                        report,
                    )
                    candidate = None
                if candidate is not None and re.fullmatch(r"-?\d+", candidate):
                    max_pk = candidate
                    snapshot_note = f"  snapshot bound: {mysql_pk[0]} <= {max_pk}"
                    if cfg.snapshot_lag_seconds > 0:
                        time.sleep(cfg.snapshot_lag_seconds)

            my_sql = build_mysql_hash_query(
                cfg, table, mysql_pk, pk_types, common_cols, pg_udt, max_pk
            )
            pg_sql = build_pg_hash_query(
                cfg, table, mysql_pk, pk_types, common_cols, pg_udt, max_pk
            )

            # Retry the streaming comparison itself on transient connection
            # drops (e.g. a network/idle timeout killing a long-running query).
            # Rebuilding the queries per attempt is cheap; re-running the whole
            # merge from scratch is the simplest correct recovery since a
            # partially-consumed merge can't be resumed mid-stream.
            for attempt in range(1, TABLE_MAX_ATTEMPTS + 1):
                try:
                    (
                        mysql_only,
                        pg_only,
                        changed,
                        changed_pks,
                        mysql_only_pks,
                        pg_only_pks,
                        order_warning,
                    ) = merge_compare(
                        iter_mysql(cfg, my_sql), iter_pg(cfg, pg_sql), cfg.sample_limit
                    )
                    break
                except Exception as exc:
                    if attempt >= TABLE_MAX_ATTEMPTS or not _is_transient_db_error(exc):
                        raise
                    print_and_write(
                        [
                            f"  WARNING: transient error on attempt {attempt}/{TABLE_MAX_ATTEMPTS}"
                            f", retrying in {TABLE_RETRY_BACKOFF_SECONDS:g}s: {exc}"
                        ],
                        report,
                    )
                    time.sleep(TABLE_RETRY_BACKOFF_SECONDS)

            if order_warning:
                # The sorted merge can't trust its own counts once a stream
                # arrives out of ascending pk_key order (see merge_compare's
                # docstring) -- silently re-verify this one table with the
                # order-independent dict_compare instead of reporting
                # possibly-bogus numbers. This is the actual fix, not just a
                # diagnostic: whatever caused the ordering violation (planner
                # choice, replica quirks, etc.), the corrected counts below
                # are trustworthy regardless of the cause.
                (
                    mysql_only,
                    pg_only,
                    changed,
                    changed_pks,
                    mysql_only_pks,
                    pg_only_pks,
                ) = dict_compare(
                    iter_mysql(cfg, my_sql), iter_pg(cfg, pg_sql), cfg.sample_limit
                )

            status = (
                "OK" if mysql_only == 0 and pg_only == 0 and changed == 0 else "DIFF"
            )
            if status == "DIFF":
                print_and_write([""], report)
            print_status_line(
                cfg, table, str(mysql_only), str(pg_only), str(changed), status, report
            )

            if status == "DIFF":
                failures.append((table, mysql_only, pg_only, changed))

                if snapshot_note:
                    print_and_write([snapshot_note], report)

                if mysql_only > 0 and mysql_only == pg_only and changed == 0:
                    # Hint when the symmetric mysql_only = pg_only pattern suggests a sort-key issue
                    print_and_write(
                        [
                            f"  NOTE: mysql_only={mysql_only} = pg_only={pg_only} with changed=0."
                            " This usually means rows are present on both sides but the PK"
                            " sort-key representations differ (e.g. UUID case, binary vs text)."
                            " Run with --inspect to confirm."
                        ],
                        report,
                    )

                if cfg.inspect_limit > 0:
                    if changed_pks:
                        inspect_changed_rows(
                            cfg,
                            table,
                            mysql_pk,
                            common_cols,
                            pg_udt,
                            changed_pks,
                            cfg.inspect_limit,
                            report,
                        )
                    if mysql_only_pks:
                        inspect_one_sided_rows(
                            cfg,
                            table,
                            mysql_pk,
                            common_cols,
                            "mysql",
                            mysql_only_pks,
                            cfg.inspect_limit,
                            report,
                        )
                    if pg_only_pks:
                        inspect_one_sided_rows(
                            cfg,
                            table,
                            mysql_pk,
                            common_cols,
                            "pg",
                            pg_only_pks,
                            cfg.inspect_limit,
                            report,
                        )
                else:
                    if changed_pks:
                        print_and_write(
                            [
                                f"  sample changed PK value(s): {', '.join(changed_pks[:10])}"
                            ],
                            report,
                        )
                    if mysql_only_pks:
                        print_and_write(
                            [
                                f"  sample MySQL-only PK value(s): {', '.join(mysql_only_pks[:10])}"
                            ],
                            report,
                        )
                    if pg_only_pks:
                        print_and_write(
                            [
                                f"  sample PG-only PK value(s): {', '.join(pg_only_pks[:10])}"
                            ],
                            report,
                        )

                print_and_write([""], report)

        except Exception as exc:
            failures.append((table, 0, 0, -1))
            print_and_write([""], report)
            print_status_line(cfg, table, "-", "-", "ERROR", "ERROR", report)
            print_and_write([f"  error: {exc}", ""], report)

    # ─── Summary ──────────────────────────────────────────────────────────────
    print_and_write(["", "=" * 88], report)
    if not failures:
        print_and_write(
            ["Summary: ALL CHECKS PASSED — no data differences found."], report
        )
    else:
        print_and_write(
            [
                f"Summary: {len(failures)} table(s) with differences or errors:",
                "",
                f"  {'TABLE':40} {'MYSQL_ONLY':>12} {'PG_ONLY':>12} {'CHANGED':>12}",
                "  " + "-" * 76,
            ],
            report,
        )
        for tname, mo, po, ch in failures:
            ch_str = str(ch) if ch >= 0 else "ERROR"
            print_and_write([f"  {tname:40} {mo:12d} {po:12d} {ch_str:>12}"], report)

    if skipped:
        print_and_write(
            ["", f"Skipped ({len(skipped)} — no common PK or columns):"], report
        )
        for s in skipped:
            print_and_write([f"  {s}"], report)

    if cfg.inspect_limit == 0 and failures:
        print_and_write(
            [
                "",
                "Tip: re-run with  --inspect 5  to see per-column diffs for changed rows.",
            ],
            report,
        )

    elapsed = datetime.now(timezone.utc) - start_time
    total_seconds = int(elapsed.total_seconds())
    h, rem = divmod(total_seconds, 3600)
    m, s = divmod(rem, 60)
    elapsed_str = f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")
    print_and_write(
        ["", "=" * 88, "", f"Full report: {report}", f"Total time:  {elapsed_str}"],
        report,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

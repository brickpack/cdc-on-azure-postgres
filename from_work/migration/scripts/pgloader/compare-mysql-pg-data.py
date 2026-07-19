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

Reports are written to $LOG_DIR/compare-mysql-pg-data_<timestamp>.txt
(default: ~/migration/logs/).
"""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

EXCLUDED_TABLES = {"flyway_schema_history", "schema_migrations"}

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


def _stream_rows(
    cmd: list[str], env: dict[str, str], label: str
) -> Iterator[tuple[str, str, str]]:
    """Stream (pk_key, pk_val, row_hash) tuples from a CLI database process."""
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    try:
        for line in proc.stdout:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t", 2)
            if len(parts) != 3:
                continue
            yield parts[0], parts[1], parts[2]
    finally:
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
    cmd, env = _pg_cmd(cfg, sql)
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
    return f"LPAD({ci}::text, 20, '0')" if dtype in _INT_PK_TYPES else f"{ci}::text"


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
) -> str:
    key_exprs = [_mysql_pk_key(c, pk_types.get(c, "")) for c in pk]
    val_exprs = [_mysql_pk_val(c) for c in pk]
    tok_exprs = [mysql_col_token(c, pg_udt.get(c, "")) for c in cols]
    order_by = ", ".join(mysql_ident(c) for c in pk)
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

    return (
        f"SELECT {pk_key_sql}, {pk_val_sql}, {row_hash} FROM {ti} ORDER BY {order_by}"
    )


def build_pg_hash_query(
    cfg: Config,
    table: str,
    pk: list[str],
    pk_types: dict[str, str],
    cols: list[str],
    pg_udt: dict[str, str],
) -> str:
    key_exprs = [_pg_pk_key(c, pk_types.get(c, "")) for c in pk]
    val_exprs = [_pg_pk_val(c) for c in pk]
    tok_exprs = [pg_col_token(c, pg_udt.get(c, "")) for c in cols]
    # Use CAST(col AS bigint) for integer PKs so the ORDER BY matches numeric sort order
    # regardless of how the column is physically stored (int, bigint, or even text).
    # Without this, a text-typed id column would sort lexicographically (10001 < 2 < 3),
    # causing the merge to produce mysql_only = pg_only = N with changed = 0.
    order_by = ", ".join(
        (
            f"CAST({pg_ident(c)} AS bigint)"
            if pk_types.get(c, "") in _INT_PK_TYPES
            else pg_ident(c)
        )
        for c in pk
    )
    ti = f"{pg_ident(cfg.pg_schema)}.{pg_ident(table)}"

    _sep = " || chr(28) || "
    pk_key_sql = key_exprs[0] if len(key_exprs) == 1 else _sep.join(key_exprs)
    pk_val_sql = val_exprs[0] if len(val_exprs) == 1 else _sep.join(val_exprs)
    row_hash = f"md5(concat_ws(chr(29), {', '.join(tok_exprs)}))"

    return (
        f"SELECT {pk_key_sql}, {pk_val_sql}, {row_hash} FROM {ti} ORDER BY {order_by}"
    )


def next_or_none(it: Iterator[tuple[str, str, str]]) -> tuple[str, str, str] | None:
    try:
        return next(it)
    except StopIteration:
        return None


def merge_compare(
    left: Iterator[tuple[str, str, str]],
    right: Iterator[tuple[str, str, str]],
    sample_limit: int,
) -> tuple[int, int, int, list[str], list[str], list[str]]:
    """Merge two pk_key-ordered streams.

    Each stream yields (pk_key, pk_val, row_hash).
    Returns (mysql_only, pg_only, changed, changed_pk_vals, mysql_only_pks, pg_only_pks).

    *_pks lists hold actual pk_val strings (not zero-padded sort keys) for up to
    sample_limit rows of each diff type, usable directly in WHERE clauses.
    """
    mysql_only = 0
    pg_only = 0
    changed = 0
    changed_pks: list[str] = []
    mysql_only_pks: list[str] = []
    pg_only_pks: list[str] = []

    l = next_or_none(left)
    r = next_or_none(right)

    while l is not None or r is not None:
        if l is None:
            _, rv, _ = r  # type: ignore[misc]
            pg_only += 1
            if len(pg_only_pks) < sample_limit:
                pg_only_pks.append(rv)
            r = next_or_none(right)
            continue
        if r is None:
            _, lv, _ = l
            mysql_only += 1
            if len(mysql_only_pks) < sample_limit:
                mysql_only_pks.append(lv)
            l = next_or_none(left)
            continue

        lk, lv, lh = l
        rk, rv, rh = r

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

    return mysql_only, pg_only, changed, changed_pks, mysql_only_pks, pg_only_pks


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


def main() -> int:
    args = parse_args()
    try:
        cfg = build_config(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
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
                print_and_write(
                    [f"{table:40} {'-':>12} {'-':>12} {'-':>12} {'SKIP-PK':>8}"], report
                )
                continue
            if mysql_pk != pg_pk:
                skipped.append(
                    f"{table}: PK mismatch  MySQL=[{','.join(mysql_pk)}]  PG=[{','.join(pg_pk)}]"
                )
                print_and_write(
                    [f"{table:40} {'-':>12} {'-':>12} {'-':>12} {'SKIP-PK':>8}"], report
                )
                continue

            pk_types = get_pk_data_types(cfg, table)
            mysql_cols = get_mysql_columns(cfg, table)
            pg_udt = get_pg_udt_map(cfg, table)
            common_cols = [c for c in mysql_cols if c in pg_udt]

            if not common_cols:
                skipped.append(f"{table}: no common columns")
                print_and_write(
                    [f"{table:40} {'-':>12} {'-':>12} {'-':>12} {'SKIP-COL':>8}"],
                    report,
                )
                continue

            my_sql = build_mysql_hash_query(
                cfg, table, mysql_pk, pk_types, common_cols, pg_udt
            )
            pg_sql = build_pg_hash_query(
                cfg, table, mysql_pk, pk_types, common_cols, pg_udt
            )

            mysql_only, pg_only, changed, changed_pks, mysql_only_pks, pg_only_pks = (
                merge_compare(
                    iter_mysql(cfg, my_sql), iter_pg(cfg, pg_sql), cfg.sample_limit
                )
            )

            status = (
                "OK" if mysql_only == 0 and pg_only == 0 and changed == 0 else "DIFF"
            )
            print_and_write(
                [
                    f"{table:40} {mysql_only:12d} {pg_only:12d} {changed:12d} {status:>8}"
                ],
                report,
            )

            if status == "DIFF":
                failures.append((table, mysql_only, pg_only, changed))

                # Hint when the symmetric mysql_only = pg_only pattern suggests a sort-key issue
                if mysql_only > 0 and mysql_only == pg_only and changed == 0:
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

        except Exception as exc:
            failures.append((table, 0, 0, -1))
            print_and_write(
                [f"{table:40} {'-':>12} {'-':>12} {'ERROR':>12} {'ERROR':>8}"], report
            )
            print_and_write([f"  error: {exc}"], report)

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

    print_and_write(["", "=" * 88, "", f"Full report: {report}"], report)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Compare row data between a MySQL source and its pg_chameleon-replicated
PostgreSQL target. Uses the same env vars / password conventions as
compare-mysql-pg.sh.

Tables with a single-column integer PK on both sides are compared with
chunked server-side checksums (the pt-table-checksum approach): each database
returns one digest row per --chunk-size range of PK values, and only chunks
whose digests differ are re-read row-by-row. In-sync tables transfer almost
nothing. All other tables are compared by fetching per-row hashes from both
sides and diffing them in memory. Both databases are always queried
concurrently, and --jobs tables are compared in parallel, so status rows
print in completion order (the summary is always in table order).

To avoid false diffs from replication lag, MAX(pk) is captured from MySQL
for every chunked table up front and both sides are compared with pk <= that
bound after a single --snapshot-lag-seconds pause (--no-snapshot-bound
disables this).

Usage:
    python3 compare-mysql-pg-data.py --env-file migration.env
        [--pg-schema ras] [--table t1,t2] [--inspect 5]
        [--jobs 8] [--chunk-size 500000]

Output per table:  TABLE  MYSQL_ONLY  PG_ONLY  CHANGED  STATUS
With --inspect N, the first N rows of each diff type are fetched and shown
with per-column values; without it, sample PK values are printed. Reports go
to $LOG_DIR/compare-mysql-pg-data_<timestamp>.txt (default ~/migration/logs/).
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

EXCLUDED_TABLES = {"flyway_schema_history", "schema_migrations"}

# Retry policy for transient connection drops during a table's comparison.
TABLE_MAX_ATTEMPTS = 3
TABLE_RETRY_BACKOFF_SECONDS = 10.0
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

# Max chunk-widths one drill-down query may span (bounds rows per query;
# nearby differing chunks share a query, bridging small gaps of clean ones).
_DRILL_MAX_CHUNKS = 5

# Non-chunkable (composite / non-integer PK) tables are buffered fully in
# memory on this host; refuse ones estimated above this instead of OOMing.
_FALLBACK_MAX_ROWS = 20_000_000

# Hash-token sentinels: CHAR(1)=NULL (distinct from empty string),
# CHAR(2)=opaque non-NULL, CHAR(28)=multi-column PK separator,
# CHAR(29)=column separator inside the row hash.
_NULL_SENTINEL = "\x01"
_OPAQUE_SENTINEL = "\x02"
_PK_SEP = "\x1c"

_INT_PK_TYPES = frozenset({"tinyint", "smallint", "mediumint", "int", "bigint"})
_PG_INT_UDTS = frozenset({"int2", "int4", "int8"})
# PG types whose text form differs from MySQL's: compared as NULL vs non-NULL only.
_OPAQUE_UDTS = frozenset({"json", "jsonb", "bytea", "bit"})


def _is_transient_db_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(p in msg for p in _TRANSIENT_DB_ERROR_PATTERNS)


@dataclass
class Config:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_db: str
    mysql_password: str
    pg_host: str
    pg_port: int
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
    jobs: int
    chunk_size: int


@dataclass
class TableMeta:
    name: str
    pk: list[str]  # PK columns (verified identical on both sides)
    pk_types: dict[str, str]  # MySQL data type per PK column
    cols: list[str]  # common columns, MySQL ordinal order
    pg_udt: dict[str, str]  # PG udt_name per column


@dataclass
class TableResult:
    table: str
    status: str  # OK | DIFF | ERROR
    mysql_only: int = 0
    pg_only: int = 0
    changed: int = 0
    lines: list[str] = field(default_factory=list)
    error: str | None = None


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
    # Source through bash so existing shell-syntax env files keep working.
    cmd = ["bash", "-lc", f"set -a; source {shlex.quote(str(env_file))}; env -0"]
    proc = subprocess.run(cmd, capture_output=True)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"Failed to source env file {path}: {stderr}")
    data: dict[str, str] = {}
    for item in proc.stdout.split(b"\0"):
        if b"=" in item:
            k, v = item.split(b"=", 1)
            data[k.decode("utf-8", "replace")] = v.decode("utf-8", "replace")
    return data


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Compare MySQL vs PostgreSQL row content"
    )
    ap.add_argument("--env-file", help="Source variables from FILE")
    ap.add_argument("--mysql-host", help="MySQL host (or MYSQL_FQDN)")
    ap.add_argument("--mysql-port", type=int, help="MySQL port (or MYSQL_PORT, default 3306)")
    ap.add_argument("--mysql-user", help="MySQL user (or MYSQL_USER)")
    ap.add_argument("--mysql-db", help="MySQL database (or MYSQL_DB)")
    ap.add_argument("--mysql-pass-file", help="Read MySQL password from FILE")
    ap.add_argument("--pg-host", help="PostgreSQL host (or PG_FQDN)")
    ap.add_argument("--pg-port", type=int, help="PostgreSQL port (or PG_PORT, default 5432)")
    ap.add_argument("--pg-user", help="PostgreSQL user (or PG_USER)")
    ap.add_argument("--pg-db", help="PostgreSQL database (or PG_DB)")
    ap.add_argument("--pg-pass-file", help="Read PostgreSQL password from FILE")
    ap.add_argument("--pg-schema", help="PostgreSQL schema (or PG_SCHEMA, default public)")
    ap.add_argument("--table", help="Comma-separated tables to compare (default: all common)")
    ap.add_argument("--sample-limit", type=int, default=20,
                    help="Max sample PK values to retain per diff type (default: 20)")
    ap.add_argument("--inspect", type=int, default=0, metavar="N",
                    help="Fetch and display up to N rows per diff type with column values")
    ap.add_argument("--jobs", type=int, default=4, metavar="N",
                    help="Tables to compare in parallel (default: 4)")
    ap.add_argument("--chunk-size", type=int, default=100_000, metavar="N",
                    help="PK-range width per checksum chunk (default: 100000)")
    ap.add_argument("--log-dir", help="Report output directory (or LOG_DIR)")
    ap.add_argument("--no-snapshot-bound", action="store_true",
                    help="Compare full live tables instead of bounding at MAX(pk)")
    ap.add_argument("--snapshot-lag-seconds", type=float, default=5.0, metavar="N",
                    help="Single pause after capturing bounds so replication catches up (default: 5)")
    return ap.parse_args()


def build_config(args: argparse.Namespace) -> Config:
    env: dict[str, str] = dict(os.environ)
    if args.env_file:
        env.update(load_env_file(args.env_file))

    mysql_password = pg_password = ""
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

    values = {
        "MYSQL_FQDN": args.mysql_host or env.get("MYSQL_FQDN", ""),
        "MYSQL_USER": args.mysql_user or env.get("MYSQL_USER", ""),
        "MYSQL_DB": args.mysql_db or env.get("MYSQL_DB", ""),
        "MYSQL_PASSWORD": mysql_password,
        "PG_FQDN": args.pg_host or env.get("PG_FQDN", ""),
        "PG_USER": args.pg_user or env.get("PG_USER", ""),
        "PG_DB": args.pg_db or env.get("PG_DB", ""),
        "PG_PASSWORD": pg_password,
    }
    missing = [k for k, v in values.items() if not v]
    if missing:
        raise RuntimeError("Missing required variables: " + " ".join(missing))

    raw = args.table.strip() if args.table else ""
    selected = {t.strip() for t in raw.split(",") if t.strip()} or None
    inspect_limit = max(0, args.inspect)

    return Config(
        mysql_host=values["MYSQL_FQDN"],
        mysql_port=args.mysql_port or int(env.get("MYSQL_PORT") or 3306),
        mysql_user=values["MYSQL_USER"],
        mysql_db=values["MYSQL_DB"],
        mysql_password=mysql_password,
        pg_host=values["PG_FQDN"],
        pg_port=args.pg_port or int(env.get("PG_PORT") or 5432),
        pg_user=values["PG_USER"],
        pg_db=values["PG_DB"],
        pg_password=pg_password,
        pg_schema=args.pg_schema or env.get("PG_SCHEMA", "public"),
        log_dir=Path(args.log_dir or env.get("LOG_DIR") or (Path.home() / "migration/logs")),
        # sample_limit must cover inspect_limit so there are enough PKs to inspect
        sample_limit=max(1, args.sample_limit, inspect_limit),
        inspect_limit=inspect_limit,
        selected_tables=selected,
        snapshot_bound=not args.no_snapshot_bound,
        snapshot_lag_seconds=max(0.0, args.snapshot_lag_seconds),
        jobs=max(1, args.jobs),
        chunk_size=max(1, args.chunk_size),
    )


# ─── Query execution ─────────────────────────────────────────────────────────


def _mysql_cmd(cfg: Config, sql: str) -> tuple[list[str], dict[str, str]]:
    cmd = [
        "mysql", "-h", cfg.mysql_host, "-P", str(cfg.mysql_port), "--protocol=TCP",
        "-u", cfg.mysql_user, "--ssl-mode=REQUIRED", "-D", cfg.mysql_db,
        # -N -B: no header, tab-separated; --quick: stream rows instead of
        # buffering the whole result set client-side.
        "-N", "-B", "--quick", "-e", sql,
    ]
    return cmd, {**os.environ, "MYSQL_PWD": cfg.mysql_password}


def _pg_cmd(cfg: Config, sql: str) -> tuple[list[str], dict[str, str]]:
    cmd = [
        "psql", "-h", cfg.pg_host, "-p", str(cfg.pg_port), "-U", cfg.pg_user,
        "-d", cfg.pg_db, "--no-password", "-At", "-F", "\t", "-c", sql,
    ]
    return cmd, {**os.environ, "PGPASSWORD": cfg.pg_password, "PGSSLMODE": "require"}


def _pg_copy_cmd(cfg: Config, select_sql: str) -> tuple[list[str], dict[str, str]]:
    """COPY ... TO STDOUT streams rows instead of buffering them in psql, and
    its text format backslash-escapes embedded tabs/newlines, so splitting
    output on a literal tab is safe."""
    cmd = [
        "psql", "-h", cfg.pg_host, "-p", str(cfg.pg_port), "-U", cfg.pg_user,
        "-d", cfg.pg_db, "--no-password",
        "-c", f"COPY ({select_sql}) TO STDOUT WITH (FORMAT text)",
    ]
    return cmd, {**os.environ, "PGPASSWORD": cfg.pg_password, "PGSSLMODE": "require"}


def _run(cmd: list[str], env: dict[str, str], label: str) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"{label} query failed")
    return proc.stdout


def run_mysql(cfg: Config, sql: str) -> str:
    return _run(*_mysql_cmd(cfg, sql), "MySQL")


def run_pg(cfg: Config, sql: str) -> str:
    return _run(*_pg_cmd(cfg, sql), "PostgreSQL")


def _run_pair(left_fn, right_fn):
    """Run two blocking callables concurrently — one per database side."""
    with ThreadPoolExecutor(max_workers=2) as ex:
        left = ex.submit(left_fn)
        right = ex.submit(right_fn)
        return left.result(), right.result()


def _fetch_hash_map(cmd: list[str], env: dict[str, str], label: str) -> dict[str, str]:
    """Stream a (pk, row_hash) query into {pk: hash} without buffering stdout."""
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env
    )
    result: dict[str, str] = {}
    for line in proc.stdout:
        parts = line.rstrip("\n").split("\t", 1)
        if len(parts) == 2:
            result[parts[0]] = parts[1]
    err = proc.stderr.read()
    if proc.wait() != 0:
        raise RuntimeError(err.strip() or f"{label} query failed")
    return result


def fetch_mysql_hash_map(cfg: Config, sql: str) -> dict[str, str]:
    return _fetch_hash_map(*_mysql_cmd(cfg, sql), "MySQL")


def fetch_pg_hash_map(cfg: Config, sql: str) -> dict[str, str]:
    return _fetch_hash_map(*_pg_copy_cmd(cfg, sql), "PostgreSQL")


# ─── Batched metadata (a few whole-schema queries, not several per table) ────


def parse_lines(raw: str) -> list[str]:
    return [ln.strip() for ln in raw.splitlines() if ln.strip()]


def _to_map_of_lists(out: str) -> dict[str, list[str]]:
    m: dict[str, list[str]] = {}
    for line in parse_lines(out):
        parts = line.split("\t")
        if len(parts) == 2:
            m.setdefault(parts[0], []).append(parts[1])
    return m


def get_mysql_tables(cfg: Config) -> dict[str, int]:
    """{table: estimated_row_count} for all base tables (estimate is rough
    but free; used only to guard the in-memory fallback path)."""
    tables: dict[str, int] = {}
    out = run_mysql(cfg, (
        "SELECT TABLE_NAME, COALESCE(TABLE_ROWS, 0) FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} AND TABLE_TYPE='BASE TABLE'"
    ))
    for line in parse_lines(out):
        parts = line.split("\t")
        if len(parts) == 2:
            tables[parts[0]] = int(parts[1])
    return tables


def get_pg_tables(cfg: Config) -> set[str]:
    return set(parse_lines(run_pg(cfg, (
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema={sql_literal(cfg.pg_schema)} AND table_type='BASE TABLE'"
    ))))


def get_mysql_pk_map(cfg: Config) -> dict[str, list[str]]:
    return _to_map_of_lists(run_mysql(cfg, (
        "SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} AND CONSTRAINT_NAME='PRIMARY' "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    )))


def get_pg_pk_map(cfg: Config) -> dict[str, list[str]]:
    return _to_map_of_lists(run_pg(cfg, (
        "SELECT tc.table_name, kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_schema=kcu.constraint_schema "
        "AND tc.constraint_name=kcu.constraint_name "
        f"WHERE tc.table_schema={sql_literal(cfg.pg_schema)} "
        "AND tc.constraint_type='PRIMARY KEY' "
        "ORDER BY tc.table_name, kcu.ordinal_position"
    )))


def get_mysql_columns_meta(cfg: Config) -> tuple[dict[str, list[str]], dict[str, dict[str, str]]]:
    """Return ({table: [cols in ordinal order]}, {table: {col: data_type}})."""
    cols: dict[str, list[str]] = {}
    types: dict[str, dict[str, str]] = {}
    out = run_mysql(cfg, (
        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA={sql_literal(cfg.mysql_db)} "
        "ORDER BY TABLE_NAME, ORDINAL_POSITION"
    ))
    for line in parse_lines(out):
        parts = line.split("\t")
        if len(parts) == 3:
            cols.setdefault(parts[0], []).append(parts[1])
            types.setdefault(parts[0], {})[parts[1]] = parts[2].lower()
    return cols, types


def get_pg_udt_maps(cfg: Config) -> dict[str, dict[str, str]]:
    udts: dict[str, dict[str, str]] = {}
    out = run_pg(cfg, (
        "SELECT table_name, column_name, udt_name FROM information_schema.columns "
        f"WHERE table_schema={sql_literal(cfg.pg_schema)}"
    ))
    for line in parse_lines(out):
        parts = line.split("\t")
        if len(parts) == 3:
            udts.setdefault(parts[0], {})[parts[1]] = parts[2]
    return udts


def get_mysql_max_pks(cfg: Config, items: list[tuple[str, str]]) -> dict[str, str]:
    """{table: MAX(pk) as string} for all (table, pk_col) items in one query."""
    if not items:
        return {}
    out = run_mysql(cfg, " UNION ALL ".join(
        f"SELECT {sql_literal(t)}, CAST(MAX({mysql_ident(c)}) AS CHAR) FROM {mysql_ident(t)}"
        for t, c in items
    ))
    result: dict[str, str] = {}
    for line in parse_lines(out):
        parts = line.split("\t", 1)
        if len(parts) == 2 and parts[1] != "NULL":
            result[parts[0]] = parts[1]
    return result


# ─── Hash / checksum SQL ─────────────────────────────────────────────────────
# Column tokens produce byte-identical NOT-NULL strings on both engines for
# equal data: bools → 'true'/'false', timestamps → 'YYYY-MM-DD HH24:MI:SS'
# (no tz / sub-second), opaque types → NULL/non-NULL sentinel only, everything
# else CAST AS CHAR / ::text with CHAR(1) for NULL.


def mysql_col_token(col: str, pg_udt: str) -> str:
    ci = mysql_ident(col)
    if pg_udt in _OPAQUE_UDTS:
        return f"CASE WHEN {ci} IS NULL THEN CHAR(1) ELSE CHAR(2) END"
    if pg_udt == "bool":
        return (f"CASE WHEN {ci} IS NULL THEN CHAR(1) "
                f"WHEN CAST({ci} AS UNSIGNED) = 0 THEN 'false' ELSE 'true' END")
    if pg_udt in ("timestamptz", "timestamp"):
        return (f"CASE WHEN {ci} IS NULL THEN CHAR(1) "
                f"ELSE DATE_FORMAT({ci}, '%Y-%m-%d %H:%i:%s') END")
    return f"COALESCE(CAST({ci} AS CHAR), CHAR(1))"


def pg_col_token(col: str, pg_udt: str) -> str:
    ci = pg_ident(col)
    if pg_udt in _OPAQUE_UDTS:
        return f"CASE WHEN {ci} IS NULL THEN chr(1) ELSE chr(2) END"
    if pg_udt == "bool":
        return (f"CASE WHEN {ci} IS NULL THEN chr(1) "
                f"WHEN {ci} THEN 'true' ELSE 'false' END")
    if pg_udt == "timestamptz":
        return (f"CASE WHEN {ci} IS NULL THEN chr(1) "
                f"ELSE to_char({ci} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') END")
    if pg_udt == "timestamp":
        return (f"CASE WHEN {ci} IS NULL THEN chr(1) "
                f"ELSE to_char({ci}, 'YYYY-MM-DD HH24:MI:SS') END")
    return f"COALESCE({ci}::text, chr(1))"


def _mysql_pk_expr(pk: list[str]) -> str:
    vals = [f"CAST({mysql_ident(c)} AS CHAR)" for c in pk]
    return vals[0] if len(vals) == 1 else "CONCAT(" + ", CHAR(28), ".join(vals) + ")"


def _pg_pk_expr(pk: list[str]) -> str:
    vals = [f"{pg_ident(c)}::text" for c in pk]
    return vals[0] if len(vals) == 1 else " || chr(28) || ".join(vals)


def _mysql_row_hash(tm: TableMeta) -> str:
    toks = ", ".join(mysql_col_token(c, tm.pg_udt.get(c, "")) for c in tm.cols)
    return f"MD5(CONCAT_WS(CHAR(29), {toks}))"


def _pg_row_hash(tm: TableMeta) -> str:
    toks = ", ".join(pg_col_token(c, tm.pg_udt.get(c, "")) for c in tm.cols)
    return f"md5(concat_ws(chr(29), {toks}))"


def _pg_table(cfg: Config, table: str) -> str:
    return f"{pg_ident(cfg.pg_schema)}.{pg_ident(table)}"


def build_mysql_hash_query(tm: TableMeta, pk_where: str | None) -> str:
    where = f" WHERE {pk_where}" if pk_where else ""
    return (f"SELECT {_mysql_pk_expr(tm.pk)}, {_mysql_row_hash(tm)} "
            f"FROM {mysql_ident(tm.name)}{where}")


def build_pg_hash_query(cfg: Config, tm: TableMeta, pk_where: str | None) -> str:
    where = f" WHERE {pk_where}" if pk_where else ""
    return (f"SELECT {_pg_pk_expr(tm.pk)}, {_pg_row_hash(tm)} "
            f"FROM {_pg_table(cfg, tm.name)}{where}")


# Chunk digests: per floor(pk / chunk_size), count(*) plus four SUMs of 32-bit
# slices of each row's MD5 — a 128-bit order-independent digest. Any changed,
# missing, or extra row alters the count or a sum with overwhelming
# probability. SUM of 32-bit values can't overflow bigint/decimal aggregates
# at any realistic chunk size, and (unlike bit_xor) exists on every MySQL and
# PostgreSQL version.


def build_mysql_chunk_query(cfg: Config, tm: TableMeta, max_pk: str | None) -> str:
    ci = mysql_ident(tm.pk[0])
    where = f" WHERE {ci} <= {max_pk}" if max_pk is not None else ""
    sums = ", ".join(
        f"SUM(CAST(CONV(SUBSTRING(h, {i * 8 + 1}, 8), 16, 10) AS UNSIGNED))"
        for i in range(4)
    )
    inner = f"SELECT {ci} AS pk, {_mysql_row_hash(tm)} AS h FROM {mysql_ident(tm.name)}{where}"
    return (f"SELECT FLOOR(pk / {cfg.chunk_size}), COUNT(*), {sums} "
            f"FROM ({inner}) AS t GROUP BY 1")


def build_pg_chunk_query(cfg: Config, tm: TableMeta, max_pk: str | None) -> str:
    ci = pg_ident(tm.pk[0])
    where = f" WHERE {ci} <= {max_pk}" if max_pk is not None else ""
    # ('x' || hex)::bit(64)::bigint zero-extends an 8-hex-digit slice into a
    # non-negative bigint, matching MySQL's CONV(..., 16, 10).
    sums = ", ".join(
        f"sum(('x' || lpad(substr(h, {i * 8 + 1}, 8), 16, '0'))::bit(64)::bigint)"
        for i in range(4)
    )
    inner = f"SELECT {ci} AS pk, {_pg_row_hash(tm)} AS h FROM {_pg_table(cfg, tm.name)}{where}"
    # floor() over numeric division matches MySQL FLOOR for negative pks too
    # (integer '/' in PG truncates toward zero instead).
    return (f"SELECT floor(pk / {cfg.chunk_size}::numeric)::bigint, count(*), {sums} "
            f"FROM ({inner}) AS t GROUP BY 1")


def parse_chunk_digests(out: str) -> dict[int, tuple[int, ...]]:
    digests: dict[int, tuple[int, ...]] = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 6:
            digests[int(parts[0])] = tuple(int(p) for p in parts[1:])
    return digests


def _drill_ranges(cids: list[int], max_span: int) -> list[tuple[int, int]]:
    """Group sorted differing chunk ids into (first, last) ranges of at most
    max_span chunk-widths."""
    ranges: list[tuple[int, int]] = []
    start = end = cids[0]
    for cid in cids[1:]:
        if cid - start + 1 <= max_span:
            end = cid
        else:
            ranges.append((start, end))
            start = end = cid
    ranges.append((start, end))
    return ranges


# ─── Comparison ──────────────────────────────────────────────────────────────

CompareCounts = tuple[int, int, int, list[str], list[str], list[str]]


def compare_maps(left: dict[str, str], right: dict[str, str], limit: int) -> CompareCounts:
    mysql_only = sorted(k for k in left if k not in right)
    pg_only = sorted(k for k in right if k not in left)
    changed = sorted(k for k in left.keys() & right.keys() if left[k] != right[k])
    return (len(mysql_only), len(pg_only), len(changed),
            changed[:limit], mysql_only[:limit], pg_only[:limit])


def _chunkable(tm: TableMeta) -> bool:
    """Chunk ids come from integer division of the PK, so it must be a native
    integer on BOTH sides (pg_chameleon can map a MySQL int to PG text/numeric)."""
    return (len(tm.pk) == 1
            and tm.pk_types.get(tm.pk[0], "") in _INT_PK_TYPES
            and tm.pg_udt.get(tm.pk[0], "") in _PG_INT_UDTS)


def compare_table_chunked(cfg: Config, tm: TableMeta, max_pk: str | None) -> CompareCounts:
    my_out, pg_out = _run_pair(
        lambda: run_mysql(cfg, build_mysql_chunk_query(cfg, tm, max_pk)),
        lambda: run_pg(cfg, build_pg_chunk_query(cfg, tm, max_pk)),
    )
    my_chunks = parse_chunk_digests(my_out)
    pg_chunks = parse_chunk_digests(pg_out)
    diff_cids = sorted(c for c in my_chunks.keys() | pg_chunks.keys()
                       if my_chunks.get(c) != pg_chunks.get(c))
    if not diff_cids:
        return (0, 0, 0, [], [], [])

    totals = [0, 0, 0]
    samples: tuple[list[str], list[str], list[str]] = ([], [], [])
    for first, last in _drill_ranges(diff_cids, _DRILL_MAX_CHUNKS):
        lo = first * cfg.chunk_size
        hi = (last + 1) * cfg.chunk_size - 1
        if max_pk is not None:
            hi = min(hi, int(max_pk))
        my_sql = build_mysql_hash_query(
            tm, f"{mysql_ident(tm.pk[0])} BETWEEN {lo} AND {hi}")
        pg_sql = build_pg_hash_query(
            cfg, tm, f"{pg_ident(tm.pk[0])} BETWEEN {lo} AND {hi}")
        left, right = _run_pair(lambda: fetch_mysql_hash_map(cfg, my_sql),
                                lambda: fetch_pg_hash_map(cfg, pg_sql))
        counts = compare_maps(left, right, cfg.sample_limit)
        for i in range(3):
            totals[i] += counts[i]
            acc = samples[i]
            if len(acc) < cfg.sample_limit:
                acc.extend(counts[i + 3][: cfg.sample_limit - len(acc)])
    return (totals[0], totals[1], totals[2], *samples)


def compare_table_full(cfg: Config, tm: TableMeta) -> CompareCounts:
    """Composite / non-integer PK fallback: fetch per-row hashes from both
    sides and diff in memory (order-independent, so no cross-engine collation
    or sort concerns)."""
    my_sql = build_mysql_hash_query(tm, None)
    pg_sql = build_pg_hash_query(cfg, tm, None)
    left, right = _run_pair(lambda: fetch_mysql_hash_map(cfg, my_sql),
                            lambda: fetch_pg_hash_map(cfg, pg_sql))
    return compare_maps(left, right, cfg.sample_limit)


# ─── Row inspection ──────────────────────────────────────────────────────────


def _norm(val: str, pg_udt: str, side: str) -> str:
    """Mirror the SQL token normalisation for the fetched raw values."""
    if side == "mysql" and val == "NULL":
        val = ""  # mysql -B prints SQL NULL as the literal string "NULL"
    if pg_udt in _OPAQUE_UDTS:
        return _OPAQUE_SENTINEL if val else _NULL_SENTINEL
    if pg_udt == "bool":
        if not val:
            return _NULL_SENTINEL
        if side == "mysql":
            return "false" if val == "0" else "true"
        return "true" if val.lower() in ("t", "true", "1", "yes", "on") else "false"
    if pg_udt in ("timestamptz", "timestamp"):
        if not val:
            return _NULL_SENTINEL
        v = val.split(".")[0]  # strip sub-second
        for sep in ("+", "-"):
            if sep in v[10:]:  # strip tz without touching the date portion
                v = v[: v.index(sep, 10)]
        return v.strip()
    return val if val else _NULL_SENTINEL


def _pk_where(pk_cols: list[str], pk_val: str, side: str) -> str:
    ident = mysql_ident if side == "mysql" else pg_ident
    return " AND ".join(
        f"{ident(c)} = {sql_literal(v)}" for c, v in zip(pk_cols, pk_val.split(_PK_SEP))
    )


def _pk_display(pk_cols: list[str], pk_val: str) -> str:
    return "  ".join(f"{c}={v}" for c, v in zip(pk_cols, pk_val.split(_PK_SEP)))


def _disp(val: str, width: int) -> str:
    return (val[: width - 1] + "…") if len(val) > width else (val or "NULL")


def _fetch_row(cfg: Config, tm: TableMeta, pk_val: str, side: str) -> list[str] | None:
    """Fetch one row's columns from the given side; None if absent."""
    if side == "mysql":
        sel = ", ".join(mysql_ident(c) for c in tm.cols)
        sql = (f"SELECT {sel} FROM {mysql_ident(tm.name)} "
               f"WHERE {_pk_where(tm.pk, pk_val, side)} LIMIT 1")
        raw = run_mysql(cfg, sql).strip()
    else:
        sel = ", ".join(pg_ident(c) for c in tm.cols)
        sql = (f"SELECT {sel} FROM {_pg_table(cfg, tm.name)} "
               f"WHERE {_pk_where(tm.pk, pk_val, side)} LIMIT 1")
        # rstrip("\n") only: strip() would eat the trailing tab psql emits
        # when the last column is NULL.
        raw = run_pg(cfg, sql).rstrip("\n")
    parts = raw.split("\t") if raw else []
    return parts if len(parts) == len(tm.cols) else None


def inspect_changed(cfg: Config, tm: TableMeta, pk_vals: list[str]) -> list[str]:
    sample = pk_vals[: cfg.inspect_limit]
    out = [f"  --- Row inspection: first {len(sample)} changed row(s) ---"]
    for pk_val in sample:
        out.append(f"  ── PK: {_pk_display(tm.pk, pk_val)}")
        try:
            mrow, prow = _run_pair(lambda: _fetch_row(cfg, tm, pk_val, "mysql"),
                                   lambda: _fetch_row(cfg, tm, pk_val, "pg"))
        except Exception as exc:
            out.append(f"    fetch error: {exc}")
            continue
        if mrow is None or prow is None:
            out.append("    row not found on one side (or column count mismatch)")
            continue
        out.append(f"  {'Column':36}  {'MySQL':45}  {'PostgreSQL':45}")
        any_diff = False
        for col, mv, pv in zip(tm.cols, mrow, prow):
            udt = tm.pg_udt.get(col, "")
            if _norm(mv, udt, "mysql") == _norm(pv, udt, "pg"):
                continue
            any_diff = True
            note = " [opaque: null vs non-null only]" if udt in _OPAQUE_UDTS else ""
            out.append(f"  {col:36}  {_disp(mv, 45):45}  {_disp(pv, 45):45}{note}")
        if not any_diff:
            out.append("    (hash differs but all columns matched after "
                       "normalisation — type may need coverage)")
        out.append("")
    return out


def inspect_one_sided(cfg: Config, tm: TableMeta, side: str, pk_vals: list[str]) -> list[str]:
    label = "MySQL" if side == "mysql" else "PostgreSQL"
    other = "pg" if side == "mysql" else "mysql"
    sample = pk_vals[: cfg.inspect_limit]
    out = [f"  --- {label}-only rows: first {len(sample)} sample(s) ---"]
    for pk_val in sample:
        out.append(f"  ── {label} PK: {_pk_display(tm.pk, pk_val)}")
        try:
            row = _fetch_row(cfg, tm, pk_val, side)
            exists_other = _fetch_row(cfg, tm, pk_val, other) is not None
        except Exception as exc:
            out.append(f"    fetch error: {exc}")
            continue
        if row is None:
            out.append("    row not found (or column count mismatch)")
            continue
        for col, val in zip(tm.cols, row):
            out.append(f"  {col:36}  {_disp(val, 55)}")
        out.append("  → same PK on other side: "
                   + ("YES (PK representation mismatch)" if exists_other
                      else "NO (row is genuinely absent)"))
        out.append("")
    return out


# ─── Per-table worker (runs in the pool; buffers output, never prints) ───────


def compare_table_worker(cfg: Config, tm: TableMeta, max_pk: str | None) -> TableResult:
    res = TableResult(table=tm.name, status="ERROR")
    try:
        for attempt in range(1, TABLE_MAX_ATTEMPTS + 1):
            try:
                if _chunkable(tm):
                    counts = compare_table_chunked(cfg, tm, max_pk)
                else:
                    counts = compare_table_full(cfg, tm)
                break
            except Exception as exc:
                if attempt >= TABLE_MAX_ATTEMPTS or not _is_transient_db_error(exc):
                    raise
                res.lines.append(
                    f"  WARNING: transient error on attempt {attempt}/{TABLE_MAX_ATTEMPTS}"
                    f", retried after {TABLE_RETRY_BACKOFF_SECONDS:g}s: {exc}")
                time.sleep(TABLE_RETRY_BACKOFF_SECONDS)
    except Exception as exc:
        res.error = str(exc)
        return res

    res.mysql_only, res.pg_only, res.changed, changed_pks, my_pks, pg_pks = counts
    res.status = "OK" if not (res.mysql_only or res.pg_only or res.changed) else "DIFF"
    if res.status != "DIFF":
        return res

    if max_pk is not None:
        res.lines.append(f"  snapshot bound: {tm.pk[0]} <= {max_pk}")
    if cfg.inspect_limit > 0:
        if changed_pks:
            res.lines += inspect_changed(cfg, tm, changed_pks)
        if my_pks:
            res.lines += inspect_one_sided(cfg, tm, "mysql", my_pks)
        if pg_pks:
            res.lines += inspect_one_sided(cfg, tm, "pg", pg_pks)
    else:
        for label, pks in (("changed", changed_pks), ("MySQL-only", my_pks),
                           ("PG-only", pg_pks)):
            if pks:
                res.lines.append(f"  sample {label} PK value(s): {', '.join(pks[:10])}")
    return res


# ─── Output ──────────────────────────────────────────────────────────────────


def print_and_write(lines: Iterable[str], out_file: Path) -> None:
    with out_file.open("a", encoding="utf-8") as f:
        for line in lines:
            print(line)
            f.write(line + "\n")


def status_row(table: str, mo: str, po: str, ch: str, status: str) -> str:
    return f"{table:40} {mo:>12} {po:>12} {ch:>12} {status:>8}"


def flush_result(res: TableResult, report: Path) -> None:
    if res.status == "ERROR":
        print_and_write(["", status_row(res.table, "-", "-", "-", "ERROR")]
                        + res.lines + [f"  error: {res.error}", ""], report)
        return
    lines = []
    if res.status == "DIFF":
        lines.append("")
    lines.append(status_row(res.table, str(res.mysql_only), str(res.pg_only),
                            str(res.changed), res.status))
    if res.lines:
        lines += res.lines + [""]
    print_and_write(lines, report)


def main() -> int:
    args = parse_args()
    try:
        cfg = build_config(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    start_time = datetime.now(timezone.utc)
    report = cfg.log_dir / f"compare-mysql-pg-data_{start_time.strftime('%Y%m%d_%H%M%S')}.txt"

    print_and_write([
        "================================================================",
        f" MySQL vs PostgreSQL data comparison — {start_time}",
        f" MySQL:      {cfg.mysql_host} / {cfg.mysql_db}",
        f" PostgreSQL: {cfg.pg_host} / {cfg.pg_db} / schema {cfg.pg_schema}",
        f" Report:     {report}",
        "================================================================",
        "",
    ], report)

    try:
        mysql_tables, pg_tables = _run_pair(lambda: get_mysql_tables(cfg),
                                            lambda: get_pg_tables(cfg))
        both = sorted((set(mysql_tables) & pg_tables) - EXCLUDED_TABLES)
        if cfg.selected_tables is not None:
            both = [t for t in both if t in cfg.selected_tables]
        if not both:
            print_and_write(["No common tables to compare."], report)
            return 0
        (mysql_pk_map, (mysql_cols, mysql_types)), (pg_pk_map, pg_udts) = _run_pair(
            lambda: (get_mysql_pk_map(cfg), get_mysql_columns_meta(cfg)),
            lambda: (get_pg_pk_map(cfg), get_pg_udt_maps(cfg)),
        )
    except Exception as exc:
        print_and_write([f"ERROR: failed to load metadata: {exc}"], report)
        return 1

    metas: list[TableMeta] = []
    skipped: list[str] = []
    skip_rows: list[tuple[str, str]] = []
    for table in both:
        mysql_pk = mysql_pk_map.get(table, [])
        pg_pk = pg_pk_map.get(table, [])
        pg_udt = pg_udts.get(table, {})
        common_cols = [c for c in mysql_cols.get(table, []) if c in pg_udt]
        if not mysql_pk or not pg_pk:
            skipped.append(f"{table}: no primary key on one or both sides")
            skip_rows.append((table, "SKIP-PK"))
        elif mysql_pk != pg_pk:
            skipped.append(f"{table}: PK mismatch  MySQL=[{','.join(mysql_pk)}]"
                           f"  PG=[{','.join(pg_pk)}]")
            skip_rows.append((table, "SKIP-PK"))
        elif not common_cols:
            skipped.append(f"{table}: no common columns")
            skip_rows.append((table, "SKIP-COL"))
        else:
            types = mysql_types.get(table, {})
            tm = TableMeta(table, mysql_pk, {c: types.get(c, "") for c in mysql_pk},
                           common_cols, pg_udt)
            if not _chunkable(tm) and mysql_tables.get(table, 0) > _FALLBACK_MAX_ROWS:
                skipped.append(
                    f"{table}: PK is not a single native integer on both sides, and "
                    f"~{mysql_tables[table]:,} rows is too large to buffer in memory")
                skip_rows.append((table, "SKIP-BIG"))
            else:
                metas.append(tm)

    # Snapshot bounds for all chunkable tables in one query, one lag pause.
    max_pks: dict[str, str] = {}
    if cfg.snapshot_bound:
        items = [(tm.name, tm.pk[0]) for tm in metas if _chunkable(tm)]
        try:
            raw = get_mysql_max_pks(cfg, items)
            max_pks = {t: v for t, v in raw.items() if re.fullmatch(r"-?\d+", v)}
        except Exception as exc:
            print_and_write([f"  WARNING: could not capture snapshot bounds: {exc}"], report)
        if max_pks and cfg.snapshot_lag_seconds > 0:
            time.sleep(cfg.snapshot_lag_seconds)

    print_and_write([
        f"Tables: {len(both)} in common",
        "",
        status_row("TABLE", "MYSQL_ONLY", "PG_ONLY", "CHANGED", "STATUS"),
        "-" * 88,
    ], report)
    for table, skip_status in skip_rows:
        print_and_write([status_row(table, "-", "-", "-", skip_status)], report)

    results: dict[str, TableResult] = {}
    with ThreadPoolExecutor(max_workers=cfg.jobs) as pool:
        futures = [pool.submit(compare_table_worker, cfg, tm, max_pks.get(tm.name))
                   for tm in metas]
        for fut in as_completed(futures):
            res = fut.result()
            results[res.table] = res
            flush_result(res, report)

    failures = [(t, r.mysql_only, r.pg_only, r.changed if r.status == "DIFF" else -1)
                for t in both if (r := results.get(t)) and r.status in ("DIFF", "ERROR")]

    print_and_write(["", "=" * 88], report)
    if not failures:
        print_and_write(["Summary: ALL CHECKS PASSED — no data differences found."], report)
    else:
        print_and_write([
            f"Summary: {len(failures)} table(s) with differences or errors:",
            "",
            f"  {'TABLE':40} {'MYSQL_ONLY':>12} {'PG_ONLY':>12} {'CHANGED':>12}",
            "  " + "-" * 76,
        ], report)
        for tname, mo, po, ch in failures:
            ch_str = str(ch) if ch >= 0 else "ERROR"
            print_and_write([f"  {tname:40} {mo:12d} {po:12d} {ch_str:>12}"], report)
    if skipped:
        print_and_write(["", f"Skipped ({len(skipped)}):"], report)
        print_and_write([f"  {s}" for s in skipped], report)
    if cfg.inspect_limit == 0 and failures:
        print_and_write(["", "Tip: re-run with  --inspect 5  to see per-column diffs."], report)

    total = int((datetime.now(timezone.utc) - start_time).total_seconds())
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    elapsed = f"{h}h {m}m {s}s" if h else (f"{m}m {s}s" if m else f"{s}s")
    print_and_write(["", "=" * 88, "",
                     f"Full report: {report}", f"Total time:  {elapsed}"], report)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

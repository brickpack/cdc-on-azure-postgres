#!/usr/bin/env python3
"""Find the actual row differences between MySQL and PostgreSQL tables.

Given a table with a row count mismatch, this script identifies the specific
primary-key values that exist on one side but not the other.

Usage:
  python3 diff-rows.py --env-file migration.env --table domain
  python3 diff-rows.py --env-file migration.env --table domain,subscription
  python3 diff-rows.py --env-file migration.env --table domain --dump 10
  python3 diff-rows.py --env-file migration.env                # all tables

Options:
  --env-file FILE          Source connection variables from FILE
  --mysql-host HOST        MySQL FQDN (or env MYSQL_FQDN)
  --mysql-user USER        MySQL user (or env MYSQL_USER)
  --mysql-db DB            MySQL source database (or env MYSQL_DB)
  --mysql-pass-file FILE   Read MySQL password from FILE
  --pg-host HOST           PostgreSQL FQDN (or env PG_FQDN)
  --pg-user USER           PostgreSQL user (or env PG_USER)
  --pg-db DB               PostgreSQL database (or env PG_DB)
  --pg-pass-file FILE      Read PG password from FILE
  --schema SCHEMA          Target PostgreSQL schema (default: from env PG_SCHEMA or 'public')
  --table TABLE[,TABLE]    Comma-separated table names to diff (default: all)
  --dump N                 Dump up to N sample missing rows from MySQL (default: 0)
  --output FILE            Write results to FILE (default: stdout only)
  --csv DIR                Export full row data for missing rows to CSV files in DIR
  --bucket-size            Row range per bucket (default: 50000) Decrease for higher precision, increase to reduce queries
"""

import argparse
import csv
import io
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ─── Database helpers ────────────────────────────────────────────────────────


def _pg_env(env):
    return {**os.environ, "PGPASSWORD": env["pg_password"], "PGSSLMODE": "require"}


def _mysql_env(env):
    return {**os.environ, "MYSQL_PWD": env["mysql_password"]}


def pg_query_raw(env, sql):
    """Run psql and return raw stdout. Returns None on error."""
    cmd = [
        "psql",
        "-h",
        env["pg_host"],
        "-U",
        env["pg_user"],
        "-d",
        env["pg_db"],
        "--no-password",
        "-At",
        "-c",
        sql,
    ]
    r = subprocess.run(cmd, capture_output=True, text=True, env=_pg_env(env))
    if r.returncode != 0:
        print(f"  PG error: {r.stderr.strip()}", file=sys.stderr)
        return None
    return r.stdout


def pg_query(env, sql, *, single_value=False):
    out = pg_query_raw(env, sql)
    if out is None:
        return None
    out = out.strip()
    if single_value:
        return out.split("\n")[0] if out else None
    return out


def mysql_query_raw(env, db, sql, *, headers=False):
    """Run mysql and return raw stdout. Returns None on error.

    Args:
        headers: If True, include column headers (omit -N flag).
    """
    cmd = [
        "mysql",
        "-h",
        env["mysql_host"],
        "-u",
        env["mysql_user"],
        "--ssl-mode=REQUIRED",
    ]
    if not headers:
        cmd.append("-N")
    cmd += ["-B", "-e", sql, db]
    r = subprocess.run(cmd, capture_output=True, env=_mysql_env(env))
    if r.returncode != 0:
        print(
            f"  MySQL error: {r.stderr.decode('utf-8', errors='replace').strip()}",
            file=sys.stderr,
        )
        return None
    return r.stdout.decode("utf-8", errors="replace")


def mysql_query(env, db, sql):
    out = mysql_query_raw(env, db, sql)
    if out is None:
        return None
    return out.strip()


def read_password_file(path):
    p = Path(path).expanduser()
    if not p.is_file():
        sys.exit(f"Cannot read password file: {path}")
    return p.read_text().strip().split("\n")[0]


# ─── PK discovery ───────────────────────────────────────────────────────────


def get_pk_columns(env, db, schema, table):
    """Return list of PK column names from MySQL. Empty list if no PK."""
    out = mysql_query(
        env,
        db,
        f"SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
        f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' "
        f"AND CONSTRAINT_NAME='PRIMARY' ORDER BY ORDINAL_POSITION",
    )
    if not out:
        return []
    return [c.strip() for c in out.split("\n") if c.strip()]


def get_table_list(env, db):
    """Get all BASE TABLEs from MySQL, excluding migration metadata."""
    skip = {"flyway_schema_history", "schema_migrations"}
    out = mysql_query(
        env,
        db,
        f"SELECT TABLE_NAME FROM information_schema.TABLES "
        f"WHERE TABLE_SCHEMA='{db}' AND TABLE_TYPE='BASE TABLE' "
        f"ORDER BY TABLE_NAME",
    )
    if not out:
        return []
    return [t.strip() for t in out.split("\n") if t.strip() and t.strip() not in skip]


def get_latin1_columns(env, db, table):
    """Return list of latin1 column names for a table in MySQL.

    utf8mb3 / utf8mb4 / ascii columns are safe.  Only latin1 (and variants
    like latin1_swedish_ci) can contain bytes > 0x7F that are not valid
    UTF-8, which pg_chameleon's decode patch replaces with \u00fffd.
    """
    out = mysql_query(
        env,
        db,
        f"SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' "
        f"AND CHARACTER_SET_NAME = 'latin1' "
        f"ORDER BY ORDINAL_POSITION",
    )
    if not out or not out.strip():
        return []
    return [c.strip() for c in out.split("\n") if c.strip()]


def check_content_table(env, db, schema, table, pk_cols, latin1_cols, sample_count=20):
    """Scan PostgreSQL for rows where latin1 columns contain the Unicode
    replacement character (U+FFFD / chr(65533)), which indicates that
    pg_chameleon's decode-error patch silently replaced non-ASCII bytes.

    Returns (corrupted_count: int, sample_lines: list[str]).
    Each sample line is a pipe-separated PK + affected column values.
    """
    # WHERE: any latin1 column contains chr(65533)
    conditions = " OR ".join(
        f'position(chr(65533) in "{c}"::text) > 0' for c in latin1_cols
    )
    pk_expr = ", ".join(f'"{c}"' for c in pk_cols)
    col_expr = pk_expr + ", " + ", ".join(f'"{c}"' for c in latin1_cols)

    count_raw = pg_query(
        env,
        f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE {conditions}',
        single_value=True,
    )
    try:
        corrupted_count = int(count_raw or "0")
    except ValueError:
        corrupted_count = 0

    samples = []
    if corrupted_count > 0:
        sample_raw = pg_query_raw(
            env,
            f'SELECT {col_expr} FROM "{schema}"."{table}" '
            f"WHERE {conditions} LIMIT {sample_count}",
        )
        if sample_raw:
            samples = [ln for ln in sample_raw.strip().split("\n") if ln.strip()]

    return corrupted_count, samples


# ─── Core diff logic ────────────────────────────────────────────────────────

# MySQL integer types that support bucket-based range diff
_INTEGER_TYPES = {"int", "bigint", "smallint", "tinyint", "mediumint"}


def _get_pk_data_type(env, db, table, pk_col):
    """Return the MySQL DATA_TYPE for a column, lowercased."""
    out = mysql_query(
        env,
        db,
        f"SELECT DATA_TYPE FROM information_schema.COLUMNS "
        f"WHERE TABLE_SCHEMA='{db}' AND TABLE_NAME='{table}' "
        f"AND COLUMN_NAME='{pk_col}' LIMIT 1",
    )
    return out.strip().lower() if out else ""


def _diff_bucketed(env, db, schema, table, pk_col, bucket_size):
    """Fast diff for a single integer PK column.

    Instead of fetching all PKs, this:
    1. Gets min/max PK from both sides (two fast index lookups).
    2. Compares row counts per equal-width bucket (one GROUP BY each side).
    3. Fetches actual PKs only for buckets where counts differ.

    For a 17M-row table with ~300 differences this finishes in seconds
    rather than transferring 17M rows over the network.
    """
    print("  Getting PK range...", flush=True)
    mysql_range = mysql_query(
        env,
        db,
        f"SELECT MIN(`{pk_col}`), MAX(`{pk_col}`) FROM `{db}`.`{table}`",
    )
    pg_range = pg_query(
        env,
        f'SELECT MIN("{pk_col}"), MAX("{pk_col}") FROM "{schema}"."{table}"',
    )

    def _parse_minmax(raw):
        if not raw:
            return None, None
        parts = raw.strip().split("\t")
        if len(parts) < 2 or not parts[0] or parts[0].upper() == "NULL":
            return None, None
        try:
            return int(parts[0]), int(parts[1])
        except (ValueError, TypeError):
            return None, None

    m_min, m_max = _parse_minmax(mysql_range)
    p_min, p_max = _parse_minmax(pg_range)

    if m_min is None and p_min is None:
        return set(), set()

    min_id = min(v for v in [m_min, p_min] if v is not None)
    max_id = max(v for v in [m_max, p_max] if v is not None)
    n_buckets = (max_id - min_id) // bucket_size + 1
    print(
        f"  PK range: {min_id:,} \u2013 {max_id:,}  →  "
        f"{n_buckets:,} bucket(s) of {bucket_size:,}",
        flush=True,
    )

    # Bucket count queries — one aggregation each side, result is tiny
    def _fetch_counts(sql, source):
        raw = mysql_query(env, db, sql) if source == "mysql" else pg_query(env, sql)
        counts = {}
        if not raw:
            return counts
        for line in raw.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) == 2:
                try:
                    counts[int(parts[0])] = int(parts[1])
                except ValueError:
                    pass
        return counts

    print("  Counting rows per bucket from MySQL...", flush=True)
    mysql_counts = _fetch_counts(
        f"SELECT FLOOR((`{pk_col}` - {min_id}) / {bucket_size}) AS b, COUNT(*) AS n "
        f"FROM `{db}`.`{table}` GROUP BY b",
        "mysql",
    )
    print("  Counting rows per bucket from PostgreSQL...", flush=True)
    pg_counts = _fetch_counts(
        f'SELECT FLOOR(("{pk_col}" - {min_id}) / {bucket_size}) AS b, COUNT(*) AS n '
        f'FROM "{schema}"."{table}" GROUP BY b',
        "pg",
    )

    all_buckets = set(mysql_counts) | set(pg_counts)
    dirty = sorted(
        b for b in all_buckets if mysql_counts.get(b, 0) != pg_counts.get(b, 0)
    )

    if not dirty:
        return set(), set()

    dirty_rows = sum(max(mysql_counts.get(b, 0), pg_counts.get(b, 0)) for b in dirty)
    print(
        f"  {len(dirty)} dirty bucket(s) out of {len(all_buckets):,}, "
        f"scanning ~{dirty_rows:,} rows",
        flush=True,
    )

    mysql_only = set()
    pg_only = set()
    for idx, b in enumerate(dirty, 1):
        lo = min_id + b * bucket_size
        hi = lo + bucket_size
        m_n = mysql_counts.get(b, 0)
        p_n = pg_counts.get(b, 0)
        print(
            f"  [{idx}/{len(dirty)}] ids [{lo:,}, {hi:,})  MySQL={m_n:,}  PG={p_n:,}",
            end="",
            flush=True,
        )
        m_raw = mysql_query_raw(
            env,
            db,
            f"SELECT `{pk_col}` FROM `{db}`.`{table}` "
            f"WHERE `{pk_col}` >= {lo} AND `{pk_col}` < {hi}",
        )
        p_raw = pg_query_raw(
            env,
            f'SELECT "{pk_col}" FROM "{schema}"."{table}" '
            f'WHERE "{pk_col}" >= {lo} AND "{pk_col}" < {hi}',
        )
        m_pks = {ln.strip() for ln in (m_raw or "").split("\n") if ln.strip()}
        p_pks = {ln.strip() for ln in (p_raw or "").split("\n") if ln.strip()}
        m_diff = m_pks - p_pks
        p_diff = p_pks - m_pks
        mysql_only |= m_diff
        pg_only |= p_diff
        print(f"  → {len(m_diff):,} missing, {len(p_diff):,} extra")

    return mysql_only, pg_only


def _diff_full_scan(env, db, schema, table, pk_cols):
    """Full PK scan diff for composite or non-integer PKs.

    Streams PKs from both sides via subprocess pipes (no full buffering
    before comparison) and prints progress every million rows.
    Composite PKs are returned as tuples; single-column as strings.
    """

    def _fetch_pk_set(source):
        if source == "mysql":
            if len(pk_cols) == 1:
                expr = f"`{pk_cols[0]}`"
            else:
                expr = "CONCAT_WS('\\t'," + ",".join(f"`{c}`" for c in pk_cols) + ")"
            sql = f"SELECT {expr} FROM `{db}`.`{table}`"
            cmd = [
                "mysql",
                "-h",
                env["mysql_host"],
                "-u",
                env["mysql_user"],
                "--ssl-mode=REQUIRED",
                "-N",
                "-B",
                "-e",
                sql,
                db,
            ]
            proc_env = _mysql_env(env)
            label = "MySQL"
        else:
            if len(pk_cols) == 1:
                expr = f'"{pk_cols[0]}"'
            else:
                expr = " || E'\\t' || ".join(f'"{c}"::text' for c in pk_cols)
            sql = f'SELECT {expr} FROM "{schema}"."{table}"'
            cmd = [
                "psql",
                "-h",
                env["pg_host"],
                "-U",
                env["pg_user"],
                "-d",
                env["pg_db"],
                "--no-password",
                "-At",
                "-c",
                sql,
            ]
            proc_env = _pg_env(env)
            label = "PG"

        print(f"  Fetching PKs from {label}...", flush=True)
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=proc_env
        )
        pks = set()
        n = 0
        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip("\n\r")
            if line:
                pks.add(tuple(line.split("\t")) if len(pk_cols) > 1 else line)
                n += 1
                if n % 1_000_000 == 0:
                    print(f"    ... {n:,} rows", flush=True)
        proc.wait()
        print(f"    {n:,} rows fetched", flush=True)
        return pks

    mysql_pks = _fetch_pk_set("mysql")
    pg_pks = _fetch_pk_set("pg")
    return mysql_pks - pg_pks, pg_pks - mysql_pks


def diff_table(env, db, schema, table, pk_cols, dump_count=0, bucket_size=50_000):
    """Diff a single table. Returns (mysql_only, pg_only, mysql_count, pg_count)."""
    print(f"\n{'='*70}")
    print(f"TABLE: {table}  (PK: {', '.join(pk_cols)})")
    print(f"{'='*70}")

    mc = mysql_query(env, db, f"SELECT COUNT(*) FROM `{db}`.`{table}`")
    pc = pg_query(env, f'SELECT COUNT(*) FROM "{schema}"."{table}"', single_value=True)

    if mc is None or pc is None:
        print("  ERROR: Could not get row counts.")
        return set(), set(), 0, 0

    mysql_count, pg_count = int(mc), int(pc)
    diff = mysql_count - pg_count
    print(f"  MySQL: {mysql_count:,}  PG: {pg_count:,}  diff: {diff:+,}")

    if diff == 0:
        print("  Row counts match — checking for swapped rows...")

    # Choose diff strategy based on PK type
    pk_type = _get_pk_data_type(env, db, table, pk_cols[0]) if len(pk_cols) == 1 else ""
    if len(pk_cols) == 1 and pk_type in _INTEGER_TYPES:
        print(
            f"  Strategy: bucket diff  (pk_type={pk_type}, bucket_size={bucket_size:,})",
            flush=True,
        )
        mysql_only, pg_only = _diff_bucketed(
            env, db, schema, table, pk_cols[0], bucket_size
        )
    else:
        reason = (
            "composite PK" if len(pk_cols) > 1 else f"pk_type={pk_type or 'unknown'}"
        )
        print(f"  Strategy: full scan  ({reason})", flush=True)
        mysql_only, pg_only = _diff_full_scan(env, db, schema, table, pk_cols)

    print(f"\n  Missing from PG (in MySQL only): {len(mysql_only):,}")
    print(f"  Extra in PG (not in MySQL):      {len(pg_only):,}")

    def _sort_key(v):
        s = "\t".join(v) if isinstance(v, tuple) else v
        return int(s) if s.lstrip("-").isdigit() else s

    if mysql_only:
        sample = sorted(mysql_only, key=_sort_key)[:20]
        print(f"\n  Sample missing PKs (up to 20):")
        for pk in sample:
            print(f"    {pk}")
        if len(mysql_only) > 20:
            print(f"    ... and {len(mysql_only) - 20:,} more")

    if pg_only:
        sample = sorted(pg_only, key=_sort_key)[:20]
        print(f"\n  Sample extra PKs in PG (up to 20):")
        for pk in sample:
            print(f"    {pk}")
        if len(pg_only) > 20:
            print(f"    ... and {len(pg_only) - 20:,} more")

    if dump_count > 0 and mysql_only:
        sample_pks = sorted(mysql_only, key=_sort_key)[:dump_count]
        print(f"\n  Row data for up to {dump_count} missing rows (from MySQL):")
        print(f"  {'-'*60}")
        for pk_val in sample_pks:
            if len(pk_cols) == 1:
                where = f"`{pk_cols[0]}` = '{pk_val}'"
            else:
                clauses = [f"`{c}` = '{v}'" for c, v in zip(pk_cols, pk_val)]
                where = " AND ".join(clauses)
            raw = mysql_query_raw(
                env,
                db,
                f"SELECT * FROM `{db}`.`{table}` WHERE {where} LIMIT 1\\G",
                headers=True,
            )
            if raw and raw.strip():
                for line in raw.strip().split("\n"):
                    print(f"    {line}")
                print()

    return mysql_only, pg_only, mysql_count, pg_count


def export_missing_csv(env, db, table, pk_cols, missing_pks, csv_dir, batch_size=500):
    """Export full row data for missing PKs from MySQL into a CSV file.

    Queries MySQL in batches using IN(...) clauses to avoid command-line length
    limits, writes proper CSV with headers.
    """
    csv_path = Path(csv_dir) / f"{table}_missing.csv"
    sorted_pks = sorted(missing_pks)
    total = len(sorted_pks)
    written = 0
    header_written = False

    with open(csv_path, "w", newline="") as fh:
        writer = None

        for i in range(0, total, batch_size):
            batch = sorted_pks[i : i + batch_size]

            if len(pk_cols) == 1:
                # Single-column PK: use IN(...)
                vals = ",".join(f"'{v}'" for v in batch)
                where = f"`{pk_cols[0]}` IN ({vals})"
            else:
                # Composite PK: use OR of ANDs
                clauses = []
                for pk_val in batch:
                    parts = [f"`{c}` = '{v}'" for c, v in zip(pk_cols, pk_val)]
                    clauses.append("(" + " AND ".join(parts) + ")")
                where = " OR ".join(clauses)

            sql = f"SELECT * FROM `{db}`.`{table}` WHERE {where}"
            raw = mysql_query_raw(env, db, sql, headers=True)
            if raw is None:
                print(f"  CSV export error at batch {i // batch_size + 1}")
                continue

            lines = raw.split("\n")
            if not lines:
                continue

            # First line from mysql --batch with headers is the header row (tab-separated)
            if not header_written and lines:
                header_fields = lines[0].split("\t")
                writer = csv.writer(fh)
                writer.writerow(header_fields)
                header_written = True
                data_lines = lines[1:]
            else:
                # Subsequent batches: skip header line
                data_lines = lines[1:] if lines else []

            for line in data_lines:
                line = line.rstrip("\r")
                if line:
                    writer.writerow(line.split("\t"))
                    written += 1

    print(f"  CSV: {written:,} rows written to {csv_path}")
    return csv_path


# ─── Env loading (same pattern as chameleon.py) ─────────────────────────────


def load_env(args):
    if args.env_file:
        p = Path(args.env_file)
        if not p.is_file():
            sys.exit(f"Env file not found: {args.env_file}")
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Strip leading 'export' keyword (shell syntax)
            if line.startswith("export "):
                line = line[len("export ") :].lstrip()
            if "=" in line:
                k, v = line.split("=", 1)
                v = v.strip().strip("'\"")
                os.environ.setdefault(k.strip(), v)

    def resolve_password(cli_file, env_key, fallback_files):
        if cli_file:
            return read_password_file(cli_file)
        val = os.environ.get(env_key)
        if val:
            return val
        for f in fallback_files:
            p = Path(f).expanduser()
            if p.is_file():
                return read_password_file(str(p))
        return None

    env = {
        "mysql_host": args.mysql_host or os.environ.get("MYSQL_FQDN", ""),
        "mysql_user": args.mysql_user or os.environ.get("MYSQL_USER", ""),
        "mysql_password": resolve_password(
            args.mysql_pass_file,
            "MYSQL_PASSWORD",
            ["~/.mysql_migration_pw", "/etc/secrets/mysql_pw"],
        ),
        "pg_host": args.pg_host or os.environ.get("PG_FQDN", ""),
        "pg_user": args.pg_user or os.environ.get("PG_USER", ""),
        "pg_db": args.pg_db or os.environ.get("PG_DB", ""),
        "pg_password": resolve_password(
            args.pg_pass_file,
            "PG_PASSWORD",
            ["~/.pg_migration_pw", "/etc/secrets/pg_pw"],
        ),
    }

    missing = [k for k, v in env.items() if not v]
    if missing:
        sys.exit(f"Missing: {', '.join(missing)}")
    return env


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Find row-level differences between MySQL and PostgreSQL tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python3 diff-rows.py --env-file migration.env --table domain
  python3 diff-rows.py --env-file migration.env --table domain,subscription --dump 5
  python3 diff-rows.py --env-file migration.env   # diff all tables with mismatches
""",
    )
    parser.add_argument("--mysql-host")
    parser.add_argument("--mysql-user")
    parser.add_argument("--mysql-db")
    parser.add_argument("--mysql-pass-file")
    parser.add_argument("--pg-host")
    parser.add_argument("--pg-user")
    parser.add_argument("--pg-db")
    parser.add_argument("--pg-pass-file")
    parser.add_argument("--schema", help="Target PostgreSQL schema")
    parser.add_argument("--env-file")
    parser.add_argument(
        "--table",
        "-t",
        help="Comma-separated table names to diff (default: all mismatched tables)",
    )
    parser.add_argument(
        "--dump",
        "-d",
        type=int,
        default=0,
        help="Dump up to N sample missing row(s) from MySQL (default: 0)",
    )
    parser.add_argument(
        "--bucket-size",
        type=int,
        default=50_000,
        metavar="N",
        help="Row range per bucket for integer PK diff (default: 50000). "
        "Smaller = more granular but more queries; larger = fewer queries.",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Write PK lists to this file (missing PKs for re-sync)",
    )
    parser.add_argument(
        "--csv",
        metavar="DIR",
        help="Export full row data for all missing rows to CSV files in DIR (one per table)",
    )
    parser.add_argument(
        "--check-content",
        action="store_true",
        help="Scan PostgreSQL for decode corruption in latin1 columns (\\ufffd replacement "
             "characters). Use after migration to validate latin1 data integrity. "
             "Automatically determines which tables/columns need checking from MySQL schema.",
    )
    args = parser.parse_args()

    env = load_env(args)
    db = args.mysql_db or os.environ.get("MYSQL_DB", "")
    schema = args.schema or os.environ.get("PG_SCHEMA", "public")
    if not db:
        sys.exit("No MySQL database specified (--mysql-db or MYSQL_DB)")

    t0 = time.time()
    print(f"MySQL: {env['mysql_host']} / {db}")
    print(f"PG:    {env['pg_host']} / {env['pg_db']} / schema {schema}")

    # ── Content corruption check mode ──────────────────────────────────────
    if args.check_content:
        tables = (
            [t.strip() for t in args.table.split(",")]
            if args.table
            else get_table_list(env, db)
        )
        print(f"\nChecking {len(tables)} table(s) for latin1 decode corruption...")
        print("(Looks for U+FFFD replacement characters written by pg_chameleon's decode patch)\n")

        total_corrupted_tables = 0
        total_corrupted_rows = 0
        risk_tables = []

        for table in tables:
            latin1_cols = get_latin1_columns(env, db, table)
            if not latin1_cols:
                print(f"  OK    {table}: no latin1 columns")
                continue

            pk_cols = get_pk_columns(env, db, schema, table)
            if not pk_cols:
                print(f"  SKIP  {table}: no primary key (cannot identify corrupted rows)")
                continue

            print(f"  Scanning {table} ({len(latin1_cols)} latin1 col(s): {', '.join(latin1_cols)})...", flush=True)
            corrupted_count, samples = check_content_table(
                env, db, schema, table, pk_cols, latin1_cols
            )

            if corrupted_count == 0:
                print(f"  OK    {table}: 0 corrupted rows")
            else:
                total_corrupted_tables += 1
                total_corrupted_rows += corrupted_count
                risk_tables.append((table, latin1_cols, corrupted_count))
                print(f"  CORRUPT {table}: {corrupted_count:,} row(s) contain \\ufffd")
                print(f"          latin1 columns: {', '.join(latin1_cols)}")
                if samples:
                    print(f"          Sample (pk | col values, up to 5):")
                    for s in samples[:5]:
                        print(f"            {s}")
                    if corrupted_count > 5:
                        print(f"            ... and {corrupted_count - 5:,} more")

        elapsed = int(time.time() - t0)
        print(f"\n{'='*70}")
        print("CONTENT CORRUPTION SUMMARY")
        print(f"{'='*70}")
        if total_corrupted_tables == 0:
            print("No decode corruption found — all latin1 columns are clean.")
        else:
            print(f"{total_corrupted_rows:,} corrupted row(s) across {total_corrupted_tables} table(s):")
            for table, cols, count in risk_tables:
                print(f"  {table:<40s} {count:>10,} row(s)  cols: {', '.join(cols)}")
            print()
            print("These rows were corrupted when pg_chameleon replaced non-UTF-8 bytes")
            print("with the Unicode replacement character (U+FFFD) during replication.")
            print("To repair: re-migrate these tables from MySQL using --csv to export,")
            print("then UPDATE or COPY the corrected rows into PostgreSQL.")
        print(f"\nTime: {elapsed // 3600}h {elapsed % 3600 // 60}m {elapsed % 60}s")
        return

    # \u2500\u2500 Normal PK diff mode \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    csv_dir = None
    if args.csv:
        csv_dir = Path(args.csv)
        csv_dir.mkdir(parents=True, exist_ok=True)
        print(f"CSV output dir: {csv_dir}")

    # Determine which tables to diff
    if args.table:
        tables = [t.strip() for t in args.table.split(",")]
    else:
        print("\nNo --table specified; scanning all tables for mismatches...")
        all_tables = get_table_list(env, db)
        tables = []
        for t in all_tables:
            mc = mysql_query(env, db, f"SELECT COUNT(*) FROM `{db}`.`{t}`")
            pc = pg_query(
                env, f'SELECT COUNT(*) FROM "{schema}"."{t}"', single_value=True
            )
            if mc is not None and pc is not None and mc != pc:
                tables.append(t)
                print(
                    f"  {t}: MySQL={int(mc):,} PG={int(pc):,} diff={int(mc)-int(pc):+,}"
                )
            elif pc is None:
                tables.append(t)
                print(f"  {t}: missing from PG")
        if not tables:
            print("\nAll tables match — nothing to diff.")
            return
        print(f"\n{len(tables)} table(s) with mismatches.")

    output_file = None
    if args.output:
        output_file = open(args.output, "w")
        print(f"\nWriting missing PKs to: {args.output}")

    summary = []

    for table in tables:
        pk_cols = get_pk_columns(env, db, schema, table)
        if not pk_cols:
            print(f"\n{'='*70}")
            print(f"TABLE: {table}")
            print(f"{'='*70}")
            print("  Skipping — no primary key found in MySQL.")
            summary.append((table, "no PK", 0, 0, 0, 0))
            continue

        mysql_only, pg_only, mc, pc = diff_table(
            env,
            db,
            schema,
            table,
            pk_cols,
            dump_count=args.dump,
            bucket_size=args.bucket_size,
        )

        summary.append(
            (table, ",".join(pk_cols), mc, pc, len(mysql_only), len(pg_only))
        )

        # Export missing rows to CSV
        if csv_dir and mysql_only:
            export_missing_csv(env, db, table, pk_cols, mysql_only, csv_dir)

        # Write missing PKs to output file
        if output_file and mysql_only:
            output_file.write(f"# TABLE: {table}  PK: {','.join(pk_cols)}\n")
            output_file.write(f"# Missing from PG ({len(mysql_only)} rows)\n")
            for pk_val in sorted(mysql_only):
                if isinstance(pk_val, tuple):
                    output_file.write("\t".join(pk_val) + "\n")
                else:
                    output_file.write(pk_val + "\n")
            output_file.write("\n")

    if output_file:
        output_file.close()

    # Summary
    elapsed = int(time.time() - t0)
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(
        f"{'TABLE':<40s} {'PK':<15s} {'MYSQL':>10s} {'PG':>10s} {'MISSING':>10s} {'EXTRA':>10s}"
    )
    print("-" * 95)
    total_missing, total_extra = 0, 0
    for table, pk, mc, pc, n_missing, n_extra in summary:
        total_missing += n_missing
        total_extra += n_extra
        flag = (
            ""
            if n_missing == 0 and n_extra == 0
            else " <<<" if pk != "no PK" else " (skipped)"
        )
        print(
            f"{table:<40s} {pk:<15s} {mc:>10,d} {pc:>10,d} {n_missing:>10,d} {n_extra:>10,d}{flag}"
        )
    print("-" * 95)
    print(
        f"{'TOTAL':<40s} {'':<15s} {'':>10s} {'':>10s} {total_missing:>10,d} {total_extra:>10,d}"
    )

    if args.output and total_missing > 0:
        print(f"\nMissing PKs written to: {args.output}")

    print(f"\nTime: {elapsed // 3600}h {elapsed % 3600 // 60}m {elapsed % 60}s")


if __name__ == "__main__":
    main()

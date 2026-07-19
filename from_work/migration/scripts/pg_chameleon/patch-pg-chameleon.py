#!/usr/bin/env python3
"""Apply all pg_chameleon patches for Azure MySQL migration.

Combines six fixes:
  1. Azure MySQL TLS/SSL support (PyMySQL ssl kwarg)
  2. Index name collision avoidance (MySQL allows index name = table name; PG doesn't)
  3. Unsigned integer type promotion (INT UNSIGNED → bigint, BIGINT UNSIGNED → numeric)
  4. Skip NOT NULL during init (allow legacy MySQL data with NULLs)
  5. Binlog decode errors (non-UTF-8 data in replication stream)
  6. Skip SAVEPOINT / non-DDL query events that crash the SQL parser

Usage (called from Dockerfile during image build):
  python3 patch-pg-chameleon.py /path/to/site-packages/pg_chameleon/lib/

Or auto-detect:
  python3 patch-pg-chameleon.py
"""

import sys
from pathlib import Path

# ─── Patch 1: Azure MySQL SSL ────────────────────────────────────────────────

SSL_MARKER = "CHAMELEON_AZURE_SSL_PATCH"

SSL_HELPER = f'''
def _pgc_mysql_ssl_kw(db_conn):
    """Added by azure-db-migration ({SSL_MARKER})."""
    if not isinstance(db_conn, dict):
        return {{}}
    v = db_conn.get("ssl")
    if v is None:
        return {{}}
    s = str(v).lower()
    if s in ("0", "false", "no", "off", ""):
        return {{}}
    return {{"ssl": {{"ssl": True}}}}
'''


def patch_ssl(mysql_lib: Path) -> bool:
    text = mysql_lib.read_text()
    if SSL_MARKER in text:
        return False
    if "class mysql_source" not in text:
        raise SystemExit(f"unexpected mysql_lib.py layout: {mysql_lib}")

    # Add helper before the class
    text = text.replace("class mysql_source", SSL_HELPER + "\nclass mysql_source", 1)

    # Patch buffered connection
    old_b = "cursorclass=pymysql.cursors.DictCursor\n        )"
    new_b = "cursorclass=pymysql.cursors.DictCursor,\n            **_pgc_mysql_ssl_kw(db_conn)\n        )"
    if old_b not in text:
        raise SystemExit("Cannot find buffered connection pattern in mysql_lib.py")
    text = text.replace(old_b, new_b, 1)

    # Patch unbuffered connection
    old_u = "cursorclass=pymysql.cursors.SSCursor\n        )"
    new_u = "cursorclass=pymysql.cursors.SSCursor,\n            **_pgc_mysql_ssl_kw(db_conn)\n        )"
    if old_u not in text:
        raise SystemExit("Cannot find unbuffered connection pattern in mysql_lib.py")
    text = text.replace(old_u, new_u, 1)

    # Patch replica connection
    old_r = 'self.replica_conn["port"] = int(db_conn["port"])'
    new_r = 'self.replica_conn["port"] = int(db_conn["port"])\n        self.replica_conn.update(_pgc_mysql_ssl_kw(db_conn))'
    if old_r not in text:
        raise SystemExit("Cannot find replica_conn pattern in mysql_lib.py")
    text = text.replace(old_r, new_r, 1)

    mysql_lib.write_text(text)
    return True


# ─── Patch 2: Index name collision ───────────────────────────────────────────

INDEX_MARKER = "CHAMELEON_INDEX_NAME_PATCH"


def patch_index_names(pg_lib: Path) -> bool:
    text = pg_lib.read_text()
    if INDEX_MARKER in text:
        return False

    target = "self.pgsql_cur.execute(idx_ddl[index])"
    if target not in text:
        print(f"WARNING: index execute pattern not found in {pg_lib} — skipping")
        return False

    # Find indentation
    indent = ""
    for line in text.splitlines():
        if target in line:
            indent = line[: len(line) - len(line.lstrip())]
            break

    replacement = "\n".join(
        [
            f"{indent}# {INDEX_MARKER}: avoid index name colliding with table name",
            f"{indent}import re as _re",
            f"{indent}_idx_sql = idx_ddl[index]",
            f'{indent}_m = _re.match(r\'CREATE\\s+(?:UNIQUE\\s+)?INDEX\\s+"?([^"\\s]+)"?\\s+ON\', _idx_sql)',
            f"{indent}if _m and _m.group(1).lower() == table.lower():",
            f"{indent}    _new_name = _m.group(1) + '_idx'",
            f"{indent}    _idx_sql = _idx_sql.replace(_m.group(1), _new_name, 1)",
            f"{indent}    self.logger.warning(",
            f"{indent}        'Index renamed to avoid collision with table name: %s -> %s on table %s',",
            f"{indent}        _m.group(1), _new_name, table,",
            f"{indent}    )",
            f"{indent}self.pgsql_cur.execute(_idx_sql)",
        ]
    )

    text = text.replace(indent + target, replacement, 1)
    pg_lib.write_text(text)
    return True


# ─── Patch 3: Unsigned integer promotion ─────────────────────────────────────

UNSIGNED_MARKER = "CHAMELEON_UNSIGNED_PATCH"

UNSIGNED_SNIPPET = """\
        # CHAMELEON_UNSIGNED_PATCH: promote unsigned integers to avoid overflow
        _unsigned_promotions = {"integer": "bigint", "bigint": "numeric"}
        col_type_str = column.get("column_type", "")
        if "unsigned" in col_type_str and column_type in _unsigned_promotions:
            _promoted = _unsigned_promotions[column_type]
            import logging as _logging
            _logging.getLogger("pg_chameleon").warning(
                "CHAMELEON_UNSIGNED_PATCH: promoted %s column '%s' from %s to %s (UNSIGNED)",
                col_type_str,
                column.get("column_name", "?"),
                column_type,
                _promoted,
            )
            column_type = _promoted"""


def patch_unsigned(pg_lib: Path) -> bool:
    text = pg_lib.read_text()
    if UNSIGNED_MARKER in text:
        return False

    # Find the return statement in get_data_type
    marker = "def get_data_type(self"
    if marker not in text:
        print(f"WARNING: get_data_type not found in {pg_lib} — skipping")
        return False

    # Insert the promotion check just before the final "return column_type" in that method.
    # Find the last "return column_type" after "def get_data_type"
    idx = text.index(marker)
    rest = text[idx:]
    target = "        return column_type"
    pos = rest.rfind(target)
    if pos == -1:
        raise SystemExit("Cannot find 'return column_type' in get_data_type")

    abs_pos = idx + pos
    text = text[:abs_pos] + UNSIGNED_SNIPPET + "\n" + text[abs_pos:]
    pg_lib.write_text(text)
    return True


# ─── Patch 4: Skip NOT NULL during init (allow NULLs in legacy data) ─────────

NOTNULL_MARKER = "CHAMELEON_SKIP_NOTNULL_PATCH"


def patch_skip_not_null(pg_lib: Path) -> bool:
    """Patch __build_create_table_mysql to skip NOT NULL constraints.

    MySQL frequently has NOT NULL columns that contain NULLs (due to historical
    lax sql_mode settings). This patch makes all columns nullable during schema
    creation so that data flows through without constraint violations.
    NOT NULL constraints can be re-applied post-migration.
    """
    text = pg_lib.read_text()
    if NOTNULL_MARKER in text:
        return False

    # Find __build_create_table_mysql and patch the is_nullable check
    method_marker = "def __build_create_table_mysql(self"
    if method_marker not in text:
        print(f"WARNING: __build_create_table_mysql not found in {pg_lib} — skipping")
        return False

    # The original code has (with varying indentation):
    #   if column["is_nullable"]=="NO":
    #           col_is_null="NOT NULL"
    #   else:
    #       col_is_null="NULL"
    #
    # We replace the condition so it always takes the else branch.
    # Use a regex to handle the inconsistent indentation in upstream.
    import re

    # Match the if/else block for is_nullable within __build_create_table_mysql
    method_start = text.index(method_marker)
    # Find the next method definition to scope our replacement
    next_def = text.find("\n    def ", method_start + 1)
    if next_def == -1:
        next_def = len(text)
    method_body = text[method_start:next_def]

    pattern = re.compile(
        r'([ \t]*)(if column\["is_nullable"\]=="NO":\s*\n)'
        r'([ \t]*col_is_null="NOT NULL"\s*\n)'
        r"([ \t]*else:\s*\n)"
        r'([ \t]*col_is_null="NULL")'
    )
    match = pattern.search(method_body)
    if not match:
        print(
            f"WARNING: is_nullable if/else pattern not found in __build_create_table_mysql — skipping"
        )
        return False

    indent = match.group(1)
    replacement_block = (
        f"{indent}# {NOTNULL_MARKER}: omit NOT NULL to allow legacy MySQL data with NULLs\n"
        f'{indent}col_is_null=""'
    )

    new_method_body = (
        method_body[: match.start()] + replacement_block + method_body[match.end() :]
    )
    text = text[:method_start] + new_method_body + text[next_def:]
    pg_lib.write_text(text)
    return True


# ─── Patch 4b: Quote replica-part identifiers ───────────────────────────────

REFRESH_PARTS_MARKER = "CHAMELEON_REFRESH_PARTS_IDENTIFIERS_PATCH"


def patch_refresh_parts_identifiers(sql_dir: Path) -> bool:
    """Patch fn_refresh_parts() so hyphenated source names produce valid SQL.

    pg_chameleon stores the source name in sch_chameleon.t_sources.v_log_table and
    then interpolates that value into constraint and index names in
    fn_refresh_parts(). When the source name contains a hyphen, the unquoted
    constraint/index identifiers become invalid SQL. Quoting those identifiers
    with %I keeps the generated names stable and valid.
    """
    create_schema = sql_dir / "create_schema.sql"
    text = create_schema.read_text()
    if REFRESH_PARTS_MARKER in text:
        return False

    if "CREATE OR REPLACE FUNCTION sch_chameleon.fn_refresh_parts()" not in text:
        print(f"WARNING: fn_refresh_parts pattern not found in {create_schema} — skipping")
        return False

    text = text.replace(
        "CONSTRAINT pk_%s PRIMARY KEY (i_id_event),",
        "CONSTRAINT %I PRIMARY KEY (i_id_event),",
        1,
    )
    text = text.replace(
        "CONSTRAINT fk_%s FOREIGN KEY (i_id_batch)",
        "CONSTRAINT %I FOREIGN KEY (i_id_batch)",
        1,
    )
    text = text.replace(
        "            CREATE INDEX IF NOT EXISTS idx_id_batch_%s",
        "            CREATE INDEX IF NOT EXISTS %I",
        1,
    )
    text = text.replace(
        "                        r_tables.v_log_table,\n                        r_tables.v_log_table,\n                        r_tables.v_log_table",
        "                        r_tables.v_log_table,\n                        'pk_' || r_tables.v_log_table,\n                        'fk_' || r_tables.v_log_table",
        1,
    )
    text = text.replace(
        "            r_tables.v_log_table,\n                        r_tables.v_log_table\n        );",
        "            'idx_id_batch_' || r_tables.v_log_table,\n                        r_tables.v_log_table\n        );",
        1,
    )
    text = text.replace(
        "CREATE OR REPLACE FUNCTION sch_chameleon.fn_refresh_parts()",
        f"-- {REFRESH_PARTS_MARKER}\nCREATE OR REPLACE FUNCTION sch_chameleon.fn_refresh_parts()",
        1,
    )

    create_schema.write_text(text)
    return True


# ─── Patch 5: Binlog decode errors (non-UTF-8 data in replication stream) ────

DECODE_MARKER = "CHAMELEON_DECODE_ERRORS_PATCH"


def patch_decode_errors(mysql_lib: Path) -> bool:
    """Patch pymysqlreplication's row_event.py to tolerate non-UTF-8 binlog data.

    MySQL databases with latin1/cp1252 columns produce binlog events containing
    bytes that are invalid UTF-8. pymysqlreplication's __read_string method uses
    strict decoding which crashes the replica. This patch changes all .decode()
    calls in row_event.py to use errors='replace'.
    """
    # Find pymysqlreplication's row_event.py relative to mysql_lib
    # mysql_lib is in pg_chameleon/lib/ — pymysqlreplication is a sibling package
    site_packages = mysql_lib.parent.parent.parent
    row_event = site_packages / "pymysqlreplication" / "row_event.py"
    if not row_event.is_file():
        # Try alternate location
        import importlib.util

        spec = importlib.util.find_spec("pymysqlreplication")
        if spec and spec.origin:
            row_event = Path(spec.origin).parent / "row_event.py"
    if not row_event.is_file():
        print(
            f"WARNING: pymysqlreplication/row_event.py not found — skipping decode patch"
        )
        return False

    text = row_event.read_text()
    if DECODE_MARKER in text:
        return False

    # Replace .decode() and .decode(errors=decode_errors) patterns to always
    # use errors='replace'. The crash happens in __read_string:
    #   string.decode(errors=decode_errors)
    # where decode_errors defaults to 'strict'.
    import re

    # Pattern 1: .decode(errors=decode_errors) → .decode(errors='replace')
    count = 0
    new_text = text

    # Replace the default value of decode_errors parameter if present
    new_text, n = re.subn(
        r"""decode_errors\s*=\s*['"]strict['"]""",
        "decode_errors='replace'",
        new_text,
    )
    count += n

    # Also replace any bare .decode() with .decode(errors='replace')
    # but only for string/bytes variables (not for known-safe patterns)
    new_text, n = re.subn(
        r"\.decode\(\)",
        ".decode(errors='replace')",
        new_text,
    )
    count += n

    if count == 0:
        # Fallback: look for the class-level default and patch it
        # Some versions define self.decode_errors = 'strict' in __init__
        new_text, n = re.subn(
            r"""self\.decode_errors\s*=\s*['"]strict['"]""",
            "self.decode_errors = 'replace'",
            new_text,
        )
        count += n

    if count == 0:
        print(f"WARNING: No decode patterns found in {row_event} — skipping")
        return False

    # Add marker comment at top
    new_text = f"# {DECODE_MARKER}\n" + new_text
    row_event.write_text(new_text)
    print(f"  Patched {count} decode pattern(s) in {row_event}")
    return True


# ─── Patch 6: Skip SAVEPOINT / non-DDL query events in replica stream ────────

SAVEPOINT_MARKER = "CHAMELEON_SKIP_SAVEPOINT_PATCH"


def patch_skip_savepoint(mysql_lib: Path) -> bool:
    """Patch __read_replica_stream to skip SAVEPOINT and other non-DDL queries.

    MySQL binlog may contain SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO, XA, and
    other transactional statements. pg_chameleon's SQL parser only understands
    DDL (ALTER, CREATE, DROP, RENAME, TRUNCATE) and crashes with a ParseError
    on anything else. The existing statement_skip list only catches exact matches
    for 'BEGIN' and 'COMMIT', but SAVEPOINT has a parameter so it doesn't match.

    This patch replaces the exact-match check with a startswith check that
    catches all transactional/non-DDL statements.
    """
    text = mysql_lib.read_text()
    if SAVEPOINT_MARKER in text:
        return False

    # The original check is:
    #   if binlogevent.query.strip().upper() not in self.statement_skip and schema_query in self.schema_mappings:
    # We replace it with a check that also skips statements starting with
    # known non-DDL keywords.
    old_check = (
        "if binlogevent.query.strip().upper() not in self.statement_skip "
        "and schema_query in self.schema_mappings:"
    )
    if old_check not in text:
        print(
            f"WARNING: statement_skip check pattern not found in {mysql_lib} — skipping"
        )
        return False

    new_check = (
        f"# {SAVEPOINT_MARKER}: skip non-DDL queries that crash the parser\n"
        "                _q_upper = binlogevent.query.strip().upper()\n"
        "                _skip_prefixes = ('SAVEPOINT', 'RELEASE', 'ROLLBACK TO', 'XA ', 'SET ', 'FLUSH', 'GRANT', 'REVOKE', 'LOCK', 'UNLOCK', 'ANALYZE', 'OPTIMIZE', 'REPAIR', 'CHECK ')\n"
        "                if _q_upper not in self.statement_skip and _q_upper.startswith(_skip_prefixes):\n"
        "                    self.logger.warning(\n"
        "                        'Skipping non-DDL binlog event (%.30s…): %.200s',\n"
        "                        _q_upper, binlogevent.query,\n"
        "                    )\n"
        "                if _q_upper not in self.statement_skip "
        "and not _q_upper.startswith(_skip_prefixes) "
        "and schema_query in self.schema_mappings:"
    )

    text = text.replace(old_check, new_check, 1)
    mysql_lib.write_text(text)
    return True


# ─── Patch 7: Tolerate unparseable DDL in replica stream ────────────────────

PARSE_ERROR_MARKER = "CHAMELEON_PARSE_ERROR_PATCH"

# Source code for the MySQL→PG translator injected into the patched mysql_lib.
# Written as a raw string so backslashes in regex patterns are preserved as-is.
_DDL_TRANSLATOR_SRC = r'''
def _mysql_ddl_to_pg(query, schema_map=None):
    """Best-effort MySQL DDL -> PostgreSQL translation for manually replaying skipped events."""
    import re as _r
    # Strip application comments (e.g. /* ApplicationName=DBeaver ... */)
    q = _r.sub(r"/\*.*?\*/", "", query.strip(), flags=_r.DOTALL).strip()
    # Backtick identifiers -> double-quoted PG identifiers
    q = q.replace(chr(96), chr(34))
    # Replace MySQL schema prefixes with PG schema names
    if schema_map:
        for _ms, _ps in schema_map.items():
            q = _r.sub(chr(34) + _r.escape(_ms) + chr(34) + r"\.", chr(34) + _ps + chr(34) + ".", q)
    # Type promotions (most-specific / longest match first)
    for _pat, _rep in [
        (r"bigint\s+unsigned",        "numeric(20,0)"),
        (r"int(?:eger)?\s+unsigned",   "bigint"),
        (r"mediumint\s+unsigned",      "integer"),
        (r"smallint\s+unsigned",       "integer"),
        (r"tinyint\s+unsigned",        "smallint"),
        (r"\bmediumint\b",             "integer"),
        (r"\btinyint\b",               "smallint"),
        (r"\bdatetime\b",              "timestamp"),
        (r"\bdouble\b",                "double precision"),
    ]:
        q = _r.sub(r"\b" + _pat, _rep, q, flags=_r.IGNORECASE)
    # Remove MySQL-only table/column options
    for _pat in [
        r"\bAUTO_INCREMENT(?:\s*=\s*\d+)?",
        r"\bENGINE\s*=\s*\S+",
        r"\bDEFAULT\s+CHARSET\s*=\s*\S+",
        r"\bCHARSET\s*=\s*\S+",
        r"\bCOLLATE\s*(?:=\s*)?\S+",
        r"\bCHARACTER\s+SET\s+\S+",
        r"\bCOMMENT\s+\'[^\']*\'",
        r"\bCOMMENT\s+\"[^\"]*\"",
    ]:
        q = _r.sub(_pat, "", q, flags=_r.IGNORECASE)
    # Collapse extra whitespace
    return _r.sub(r"\s+", " ", q).strip()
'''


def patch_parse_error(mysql_lib: Path) -> bool:
    """Wrap sql_tokeniser.parse_sql() in a try/except so that DDL statements
    the parser cannot handle (e.g. ALTER TABLE … CHECK (…)) are logged and
    skipped instead of crashing the entire read daemon.

    Root cause: pg_chameleon uses the `parsy` library to tokenise DDL events
    read from the MySQL binlog. Any MySQL DDL syntax it doesn't recognise raises
    parsy.ParseError, which propagates unhandled and kills the read process,
    putting the source into error state permanently.
    """
    text = mysql_lib.read_text()
    if PARSE_ERROR_MARKER in text:
        return False

    # The call site we need to wrap is:
    #   sql_tokeniser.parse_sql(binlogevent.query)
    # It appears inside __read_replica_stream.
    old_call = "sql_tokeniser.parse_sql(binlogevent.query)"
    if old_call not in text:
        print(
            f"WARNING: parse_sql call pattern not found in {mysql_lib} — skipping"
        )
        return False

    # Find indentation of that line
    indent = ""
    for line in text.splitlines():
        if old_call in line:
            indent = line[: len(line) - len(line.lstrip())]
            break

    # Inject the translator function indented to match the surrounding code,
    # then add the try/except wrapper around parse_sql.
    translator_block = "\n".join(
        (indent + line) if line.strip() else ""
        for line in _DDL_TRANSLATOR_SRC.splitlines()
    ) + "\n"

    new_call = (
        f"{indent}# {PARSE_ERROR_MARKER}: skip DDL that the parser cannot handle\n"
        + translator_block
        + f"{indent}try:\n"
        f"{indent}    sql_tokeniser.parse_sql(binlogevent.query)\n"
        f"{indent}except Exception as _parse_exc:\n"
        f"{indent}    self.logger.warning(\n"
        f"{indent}        \"Skipping unparseable DDL event (ParseError): %s — query: %.200s\",\n"
        f"{indent}        _parse_exc,\n"
        f"{indent}        binlogevent.query,\n"
        f"{indent}    )\n"
        f"{indent}    # Write translated PG SQL to a per-source skip log on the bind-mounted data dir.\n"
        f"{indent}    # One file per source — no shared file handles across containers.\n"
        f"{indent}    try:\n"
        f"{indent}        import datetime as _dt\n"
        f"{indent}        _src = getattr(self, 'source', 'unknown')\n"
        f"{indent}        _sm  = getattr(self, 'schema_mappings', None)\n"
        f"{indent}        _schemas = list(_sm.values()) if _sm else []\n"
        f"{indent}        _schema  = _schemas[0] if _schemas else None\n"
        f"{indent}        _pg_ddl  = _mysql_ddl_to_pg(binlogevent.query, _sm)\n"
        f"{indent}        _ddl_log = f'/data/migration/chameleon/skipped_ddl_{{_src}}.sql'\n"
        f"{indent}        _ts = _dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')\n"
        f"{indent}        with open(_ddl_log, 'a') as _f:\n"
        f"{indent}            _f.write('-- ' + '=' * 60 + '\\n')\n"
        f"{indent}            _f.write(f'-- Skipped {{_ts}}  source={{_src}}\\n')\n"
        f"{indent}            _f.write(f'-- Error:  {{_parse_exc}}\\n')\n"
        f"{indent}            if _schema:\n"
        f"{indent}                _f.write(f'-- Schema: {{_schema}}\\n')\n"
        f"{indent}            _f.write('-- ' + '=' * 60 + '\\n')\n"
        f"{indent}            _f.write('-- MySQL original:\\n-- ')\n"
        f"{indent}            _f.write(binlogevent.query.strip().replace('\\n', '\\n-- '))\n"
        f"{indent}            _f.write('\\n-- PostgreSQL translation (verify before running):\\n')\n"
        f"{indent}            if _schema:\n"
        f"{indent}                _f.write(f'SET search_path = {{_schema}};\\n')\n"
        f"{indent}            _f.write(_pg_ddl + ';\\n\\n')\n"
        f"{indent}    except Exception:\n"
        f"{indent}        pass  # never let file I/O crash the read daemon"
    )

    text = text.replace(indent + old_call, new_call, 1)
    mysql_lib.write_text(text)
    return True


# ─── Main ────────────────────────────────────────────────────────────────────


def find_lib_dir() -> Path:
    """Auto-detect pg_chameleon lib directory."""
    import pg_chameleon

    return Path(pg_chameleon.__file__).parent / "lib"


def main():
    if len(sys.argv) > 1:
        lib_dir = Path(sys.argv[1])
    else:
        lib_dir = find_lib_dir()

    sql_dir = lib_dir.parent / "sql"

    mysql_lib = lib_dir / "mysql_lib.py"
    pg_lib = lib_dir / "pg_lib.py"

    if not mysql_lib.is_file():
        raise SystemExit(f"Not found: {mysql_lib}")
    if not pg_lib.is_file():
        raise SystemExit(f"Not found: {pg_lib}")

    results = []
    if patch_ssl(mysql_lib):
        results.append("SSL")
    if patch_decode_errors(mysql_lib):
        results.append("decode-errors")
    if patch_index_names(pg_lib):
        results.append("index-names")
    if patch_unsigned(pg_lib):
        results.append("unsigned-types")
    if patch_skip_not_null(pg_lib):
        results.append("skip-not-null")
    if patch_refresh_parts_identifiers(sql_dir):
        results.append("refresh-parts-identifiers")
    if patch_skip_savepoint(mysql_lib):
        results.append("skip-savepoint")
    if patch_parse_error(mysql_lib):
        results.append("parse-error-skip")

    if results:
        print(f"Patched: {', '.join(results)}")
    else:
        print("All patches already applied.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""pg_chameleon migration operator — replicate MySQL → PostgreSQL.

Usage:
  python3 chameleon.py start [options]       Start replication containers
  python3 chameleon.py status [options]      Quick status overview
  python3 chameleon.py detail [options]      Detailed replication status (batches, lag, errors)
  python3 chameleon.py logs [options]        Show recent container logs
  python3 chameleon.py stop [options]        Stop replication containers
  python3 chameleon.py fix-sequences [options]    Sync PG sequences to max(id)
  python3 chameleon.py apply-fks [options]        Re-apply foreign keys from MySQL
  python3 chameleon.py drop-not-nulls [options]   Drop NOT NULL where MySQL has NULLs
  python3 chameleon.py apply-not-nulls [options]  Re-apply NOT NULL constraints
  python3 chameleon.py apply-defaults [options]   Copy MySQL column DEFAULTs to PG
  python3 chameleon.py apply-comments [options]   Copy MySQL table/column COMMENTs to PG
  python3 chameleon.py validate [options]     Compare MySQL/PG row counts, show discards & errors
  python3 chameleon.py diagnose [options]     Pre-flight checks: binlog, grants, GTID, connectivity
  python3 chameleon.py reset [options]       Drop catalogue & schemas, start fresh

Connection options (or set via env / --env-file):
  --mysql-host, --mysql-user, --mysql-db, --mysql-pass-file
  --pg-host, --pg-user, --pg-db, --pg-pass-file
  --schema SCHEMA  (target PostgreSQL schema, default: public)
  --source NAME  (operate on one source only)
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import yaml

IMAGE = os.environ.get("CHAMELEON_IMAGE", "pg-chameleon:latest")
CONFIG_NAME = "default"
SKIP_TABLE_NAMES = {"flyway_schema_history", "schema_migrations"}


def _pg_source_name(db):
    """Sanitize a MySQL database name for use as a pg_chameleon source name.

    pg_chameleon embeds the source name in unquoted PostgreSQL identifier names
    (table names, constraint names).  Hyphens are not valid in unquoted PG
    identifiers and cause a SyntaxError, so they are replaced with underscores.
    The original MySQL DB name is still used in schema_mappings and MySQL queries.
    """
    return db.replace("-", "_")


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _pg_env(env):
    """Env dict for psql commands."""
    return {**os.environ, "PGPASSWORD": env["pg_password"], "PGSSLMODE": "require"}


def _psql_base(env):
    """Base psql command with connection args."""
    return [
        "psql",
        "-h",
        env["pg_host"],
        "-U",
        env["pg_user"],
        "-d",
        env["pg_db"],
        "--no-password",
    ]


def psql_print(env, sql):
    """Run psql with formatted (human-readable) output."""
    subprocess.run(_psql_base(env) + ["-c", sql], env=_pg_env(env))


def pg_query(env, sql, *, single_value=False):
    """Run a psql query and return output."""
    cmd = _psql_base(env) + ["-At", "-c", sql]
    r = subprocess.run(cmd, capture_output=True, text=True, env=_pg_env(env))
    if r.returncode != 0:
        return None
    out = r.stdout.strip()
    if single_value:
        return out.split("\n")[0] if out else None
    return out


def pg_exec(env, sql):
    """Execute SQL on PostgreSQL (no output expected)."""
    cmd = _psql_base(env) + ["-v", "ON_ERROR_STOP=1", "-c", sql]
    return subprocess.run(cmd, capture_output=True, text=True, env=_pg_env(env))


def mysql_query(env, db, sql):
    """Run a MySQL query and return output."""
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
    mysql_env = {**os.environ, "MYSQL_PWD": env["mysql_password"]}
    r = subprocess.run(cmd, capture_output=True, text=True, env=mysql_env)
    if r.returncode != 0:
        return None
    return r.stdout.strip()


PASSWORD_FILES = [
    "~/.mysql_migration_pw",
    "~/.pg_migration_pw",
]


def read_password_file(path):
    """Read first line of a password file."""
    p = Path(path)
    if not p.is_file():
        sys.exit(f"Cannot read password file: {path}")
    return p.read_text().strip().split("\n")[0]


def shred_password_files():
    """Overwrite and remove password files."""
    for f in PASSWORD_FILES:
        p = Path(f).expanduser()
        if p.is_file():
            # Overwrite before unlinking (best-effort on non-SSD)
            size = p.stat().st_size
            p.write_bytes(b"\x00" * size)
            p.unlink()
            print(f"  Shredded: {p}")


def docker_image_exists(image):
    r = subprocess.run(
        ["docker", "image", "inspect", image],
        capture_output=True,
        check=False,
    )
    return r.returncode == 0


def container_running(name, *, include_stopped=False):
    """Return True if the Docker container exists (and optionally is running)."""
    flag = "-aq" if include_stopped else "-q"
    r = subprocess.run(
        ["docker", "ps", flag, "--filter", f"name=^{name}$"],
        capture_output=True,
        text=True,
    )
    return bool(r.stdout.strip())


def _container_name(env, src):
    """Unique replica container name scoped to both PG database and MySQL source.

    Including pg_db prevents collisions when two env files replicate from the
    same MySQL source into different PG databases (e.g. license → license and
    license → ras_license would otherwise share the same container name and
    kill each other on start).
    """
    return f"chameleon-replica-{env['pg_db']}-{src}"


def _stop_containers(env, sources):
    """Stop and remove replica containers. Best-effort — silent if missing."""
    for src in sources:
        container = _container_name(env, src)
        subprocess.run(["docker", "stop", container], capture_output=True, check=False)
        subprocess.run(
            ["docker", "rm", "-f", container], capture_output=True, check=False
        )


def chameleon_home(env):
    home = Path.home() / ".pg_chameleon" / env["pg_db"]
    home.mkdir(parents=True, exist_ok=True)
    return home


def data_dir(env):
    d = Path(
        os.environ.get(
            "CHAMELEON_DATA",
            str(Path.home() / "migration" / "chameleon" / env["pg_db"]),
        )
    )
    for sub in ("copy", "pid", "logs"):
        (d / sub).mkdir(parents=True, exist_ok=True)
    return d


def _docker_run_args(env, *, name=None, detach=False, restart=None):
    """Common docker run args for chameleon containers."""
    home = chameleon_home(env)
    dd = data_dir(env)
    mem_limit = os.environ.get("CHAMELEON_MEMORY_LIMIT", "1g")
    args = ["docker", "run"]
    if detach:
        args += ["-d"]
    else:
        args += ["--rm"]
    if name:
        args += ["--name", name]
    if restart:
        args += ["--restart", restart]
    if detach:
        args += [
            "--log-driver",
            "json-file",
            "--log-opt",
            "max-size=50m",
            "--log-opt",
            "max-file=3",
        ]
    args += [
        "--network",
        "host",
        "--memory",
        mem_limit,
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "-e",
        "PYTHONWARNINGS=ignore::DeprecationWarning",
        "-v",
        f"{home}:/chameleon/.pg_chameleon",
        "-v",
        f"{dd}:/data/migration/chameleon",
        "-v",
        "/etc/ssl/certs:/etc/ssl/certs:ro",
        IMAGE,
        "--config",
        CONFIG_NAME,
    ]
    return args


def chameleon_cmd(env, *args):
    """Run chameleon in Docker. Returns subprocess.CompletedProcess."""
    cmd = _docker_run_args(env) + list(args)
    return subprocess.run(cmd, check=False)


def catalogue_exists(env):
    r = pg_query(
        env,
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name='sch_chameleon';",
        single_value=True,
    )
    return r == "1"


def source_registered(env, source):
    if not catalogue_exists(env):
        return False
    r = pg_query(
        env,
        f"SELECT count(*) FROM sch_chameleon.t_sources WHERE t_source='{source}';",
        single_value=True,
    )
    return r == "1"


# ─── Config writer ────────────────────────────────────────────────────────────


def write_config(env, sources, schema_map):
    """Write pg_chameleon YAML config."""
    home = chameleon_home(env)
    data_dir(env)  # ensure subdirs exist
    cfg_path = home / "configuration" / f"{CONFIG_NAME}.yml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    src_configs = {}
    for i, db in enumerate(sources):
        schema = schema_map[db]
        src_configs[_pg_source_name(db)] = {
            "type": "mysql",
            "db_conn": {
                "host": env["mysql_host"],
                "port": "3306",
                "user": env["mysql_user"],
                "password": env["mysql_password"],
                "charset": "utf8",
                "connect_timeout": 30,
                "ssl": "required",
            },
            "schema_mappings": {db: schema},
            "limit_tables": [],
            "skip_tables": [f"{db}.{t}" for t in sorted(SKIP_TABLE_NAMES)],
            "grant_select_to": [],
            "lock_timeout": "120s",
            "my_server_id": 200 + i,
            "replica_batch_size": int(
                os.environ.get("CHAMELEON_REPLICA_BATCH_SIZE", "10000")
            ),
            "replay_max_rows": int(
                os.environ.get("CHAMELEON_REPLAY_MAX_ROWS", "10000")
            ),
            "batch_retention": os.environ.get("CHAMELEON_BATCH_RETENTION", "3 days"),
            "copy_max_memory": os.environ.get("CHAMELEON_COPY_MAX_MEMORY", "30M"),
            "copy_mode": os.environ.get("CHAMELEON_COPY_MODE", "file"),
            "out_dir": "/data/migration/chameleon/copy",
            "sleep_loop": 1,
            "on_error_replay": os.environ.get("CHAMELEON_ON_ERROR_REPLAY", "exit"),
            "on_error_read": os.environ.get("CHAMELEON_ON_ERROR_READ", "exit"),
            "auto_maintenance": os.environ.get(
                "CHAMELEON_AUTO_MAINTENANCE", "disabled"
            ),
            "gtid_enable": os.environ.get("CHAMELEON_GTID_ENABLE", "true").lower()
            in ("true", "1", "yes"),
            "skip_events": {"insert": [], "delete": [], "update": []},
            "keep_existing_schema": False,
            "net_read_timeout": int(
                os.environ.get("CHAMELEON_NET_READ_TIMEOUT", "3600")
            ),
            "net_write_timeout": int(
                os.environ.get("CHAMELEON_NET_WRITE_TIMEOUT", "3600")
            ),
            "wait_timeout": int(os.environ.get("CHAMELEON_WAIT_TIMEOUT", "28800")),
        }

    cfg = {
        "pid_dir": "/data/migration/chameleon/pid",
        "log_dir": "/data/migration/chameleon/logs",
        "log_dest": "stdout",
        "log_level": os.environ.get("CHAMELEON_LOG_LEVEL", "info"),
        "log_days_keep": 10,
        "rollbar_key": "",
        "rollbar_env": "",
        "type_override": {
            "tinyint(1)": {"override_to": "boolean", "override_tables": ["*"]},
        },
        "pg_conn": {
            "host": env["pg_host"],
            "port": "5432",
            "user": env["pg_user"],
            "password": env["pg_password"],
            "database": env["pg_db"],
            "charset": "utf8",
        },
        "sources": src_configs,
    }

    cfg_path.write_text(yaml.safe_dump(cfg, default_flow_style=False, sort_keys=False))
    cfg_path.chmod(0o600)
    print(f"Config written: {cfg_path}")


# ─── Commands ─────────────────────────────────────────────────────────────────


def cmd_start(env, sources, schema_map):
    if not docker_image_exists(IMAGE):
        sys.exit(f"Docker image {IMAGE} not found. Build it first.")

    # Verify connectivity
    print("Checking MySQL...")
    r = mysql_query(env, sources[0], "SELECT 1")
    if r is None:
        sys.exit(f"MySQL connection failed: {env['mysql_user']}@{env['mysql_host']}")

    print("Checking PostgreSQL...")
    r = pg_query(env, "SELECT 1", single_value=True)
    if r is None:
        sys.exit(f"PostgreSQL connection failed: {env['pg_user']}@{env['pg_host']}")

    write_config(env, sources, schema_map)

    dd = data_dir(env)

    # Sanity-check: Docker memory limit must exceed copy_max_memory + runtime overhead.
    # Exit 137 during init_replica means the container was OOM-killed by Docker.
    def _to_bytes(s):
        s = s.strip().upper()
        if s.endswith("G"):
            return int(s[:-1]) * 1024**3
        if s.endswith("M"):
            return int(s[:-1]) * 1024**2
        if s.endswith("K"):
            return int(s[:-1]) * 1024
        return int(s)

    mem_limit_str = os.environ.get("CHAMELEON_MEMORY_LIMIT", "1g")
    copy_max_str = os.environ.get("CHAMELEON_COPY_MAX_MEMORY", "300M")
    try:
        mem_bytes = _to_bytes(mem_limit_str)
        copy_bytes = _to_bytes(copy_max_str)
        if mem_bytes < copy_bytes * 2:
            print(
                f"  WARNING: CHAMELEON_MEMORY_LIMIT ({mem_limit_str}) is less than "
                f"2x CHAMELEON_COPY_MAX_MEMORY ({copy_max_str}). "
                "init_replica may be OOM-killed (exit 137). "
                "Increase CHAMELEON_MEMORY_LIMIT or reduce CHAMELEON_COPY_MAX_MEMORY."
            )
    except (ValueError, TypeError):
        pass

    print(f"\n{'='*60}")
    print("pg_chameleon replication")
    print(f"{'='*60}")
    print(f"  Data dir: {dd}")
    print(f"  Logs: docker logs -f chameleon-replica-{env['pg_db']}-<source>")
    for db in sources:
        print(f"  {db} → {env['pg_host']}/{env['pg_db']} schema={schema_map[db]}")
    print()

    # Create catalogue if needed
    if not catalogue_exists(env):
        print("Creating replica schema...")
        chameleon_cmd(env, "create_replica_schema")

    # Fix: allow NULLs in t_error_log.t_table_pkey to prevent cascade crashes
    pg_exec(
        env,
        "ALTER TABLE IF EXISTS sch_chameleon.t_error_log "
        "ALTER COLUMN t_table_pkey DROP NOT NULL;",
    )

    for src in sources:
        print(f"\n--- Source: {src} ---")
        schema = schema_map[src]

        if not source_registered(env, _pg_source_name(src)):
            print(f"Adding source: {src}")
            chameleon_cmd(env, "add_source", "--source", _pg_source_name(src))

        # Check status
        st = pg_query(
            env,
            f"SELECT enm_status FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}';",
            single_value=True,
        )

        if st == "error":
            print("Clearing error status...")
            chameleon_cmd(env, "enable_replica", "--source", _pg_source_name(src))
            st = pg_query(
                env,
                f"SELECT enm_status FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}';",
                single_value=True,
            )

        # Check if init already done
        cat_count = (
            pg_query(
                env,
                f"""
            SELECT count(*) FROM sch_chameleon.t_replica_tables
            WHERE i_id_source=(SELECT i_id_source FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}');
        """,
                single_value=True,
            )
            or "0"
        )

        if int(cat_count) > 0:
            print(f"Init already done ({cat_count} tables); skipping init_replica")
        else:
            print(f"Running init_replica (this may take hours)...")
            pg_exec(env, f"DROP SCHEMA IF EXISTS _{schema}_tmp CASCADE;")
            pg_exec(
                env, f"DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};"
            )
            # Give the init container a recognisable name so it's visible in
            # 'docker ps' during the (potentially hours-long) copy phase.
            # --rm means it self-cleans on exit.
            init_container = f"chameleon-init-{env['pg_db']}-{src}"
            subprocess.run(
                ["docker", "rm", "-f", init_container], capture_output=True, check=False
            )
            init_cmd = _docker_run_args(env, name=init_container) + [
                "init_replica",
                "--source",
                _pg_source_name(src),
            ]
            result = subprocess.run(init_cmd, check=False)
            if result.returncode != 0:
                print(
                    f"ERROR: init_replica failed for {src} (exit {result.returncode}). Skipping replica start."
                )
                continue

        # Validate binlog position — but skip this check when the source has already
        # reached a consistent state (b_consistent=t). With GTID replication,
        # pg_chameleon clears t_binlog_name once consistent; position is tracked
        # internally via the GTID set. An empty binlog here is normal and safe.
        consistent_val = pg_query(
            env,
            f"SELECT b_consistent FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}';",
            single_value=True,
        )
        already_consistent = consistent_val and consistent_val.strip() == "t"
        binlog_name = pg_query(
            env,
            f"SELECT t_binlog_name FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}';",
            single_value=True,
        )
        if not already_consistent and (not binlog_name or not binlog_name.strip()):
            print(
                f"\n  ERROR: Source '{src}' has NO binlog position after init_replica."
            )
            print("  start_replica will run but replicate NOTHING.")
            print("  Run 'python3 chameleon.py diagnose' to identify the cause.")
            print(
                "  Likely fix: grant REPLICATION CLIENT + REPLICATION SLAVE to MySQL user,"
            )
            print("  or set CHAMELEON_GTID_ENABLE=true if MySQL has gtid_mode=ON.")
            print(
                f"  Then: python3 chameleon.py reset ... && python3 chameleon.py start ..."
            )
            continue

        # Start replica in detached container
        container = _container_name(env, src)
        # Also remove the legacy container name (chameleon-replica-{src}) that was
        # used before pg_db was included in the name — prevents server_id conflicts
        # when two sources share the same MySQL source name in different pg databases.
        legacy_container = f"chameleon-replica-{src}"
        for c in (container, legacy_container):
            # Disable restart policy first so Docker won't resurrect the container
            # between stop and rm (race condition with --restart unless-stopped).
            subprocess.run(
                ["docker", "update", "--restart=no", c],
                capture_output=True,
                check=False,
            )
            # Use SIGKILL directly — avoids the daemon bug where stop sends SIGTERM
            # then SIGKILL but never receives the exit event, leaving the container
            # in an unkillable state ("could not kill container").
            subprocess.run(
                ["docker", "kill", "--signal=9", c],
                capture_output=True,
                check=False,
            )
            time.sleep(1)
            rm = subprocess.run(
                ["docker", "rm", "-f", c], capture_output=True, text=True, check=False
            )
            if rm.returncode != 0 and rm.stderr.strip():
                # Only report genuine errors (non-zero exit with a message); a
                # "No such container" result is fine and produces no stderr.
                print(f"  WARN: docker rm -f {c}: {rm.stderr.strip()}")

        # Verify the main container is actually gone before attempting docker run.
        if container_running(container, include_stopped=True):
            print(
                f"\n  ERROR: Container '{container}' could not be removed automatically."
            )
            print(f"  The Docker daemon may have lost the container's exit event.")
            print(f"  Recovery options (try in order):")
            print(f"    1. docker rm -f {container}")
            print(f"    2. sudo systemctl restart docker  # if step 1 fails")
            print(f"  Then re-run: python3 chameleon.py start ...")
            continue

        # Remove any stale PID file before starting.  pg_chameleon writes
        # {pid_dir}/default_{source}.pid containing its process PID.  Inside a
        # Docker container the main process always has PID 1, so on restart the
        # new container finds the old PID file, sees PID 1 is running (itself),
        # concludes another instance is already active, and exits with code 0.
        # Docker then restarts it — creating an infinite restart loop.
        stale_pid = data_dir(env) / "pid" / f"{CONFIG_NAME}_{_pg_source_name(src)}.pid"
        if stale_pid.exists():
            stale_pid.unlink()
            print(f"  Removed stale PID file: {stale_pid}")

        print(f"Starting replica container: {container}")
        cmd = _docker_run_args(
            env, name=container, detach=True, restart="unless-stopped"
        )
        cmd += ["start_replica", "--source", _pg_source_name(src)]
        subprocess.run(cmd, check=True)

        # Health check: wait a few seconds and verify container is still running
        time.sleep(5)
        if not container_running(container):
            print(f"\n  WARNING: Container {container} exited immediately!")
            print(f"  Last 30 lines of container log:")
            subprocess.run(
                ["docker", "logs", "--tail", "30", container],
            )
            print()

    print("\nDone. Use: python3 chameleon.py status")


def cmd_status(env, sources, schema_map):
    print(f"Database: {env['pg_host']}/{env['pg_db']}\n")

    if not catalogue_exists(env):
        print("No sch_chameleon schema — migration not initialised.")
        return

    psql_print(
        env,
        "SELECT t_source, enm_status, b_consistent, t_binlog_name, i_binlog_position "
        "FROM sch_chameleon.t_sources ORDER BY i_id_source;",
    )
    # With GTID replication, t_binlog_name/i_binlog_position are cleared once the
    # replica reaches a consistent state (b_consistent=t). This is normal — position
    # is tracked via the GTID set, not a file+offset.
    gtid_on = os.environ.get("CHAMELEON_GTID_ENABLE", "true").lower() in (
        "true",
        "1",
        "yes",
    )
    if gtid_on:
        print("  (empty binlog/pos is normal with GTID once b_consistent=t)")

    err_count = (
        pg_query(
            env,
            "SELECT count(*) FROM sch_chameleon.t_error_log WHERE ts_error > now() - interval '24 hours';",
            single_value=True,
        )
        or "0"
    )
    if err_count != "0":
        print(f"\nErrors in last 24h: {err_count}")
        psql_print(
            env,
            "SELECT e.ts_error, s.t_source, e.v_table_name, "
            "substring(e.t_error_message from 1 for 120) AS error "
            "FROM sch_chameleon.t_error_log e "
            "JOIN sch_chameleon.t_sources s ON s.i_id_source = e.i_id_source "
            "WHERE e.ts_error > now() - interval '24 hours' "
            "ORDER BY e.ts_error DESC LIMIT 5;",
        )
    else:
        print("No errors in last 24h.")

    # Container status
    print()
    for src in sources:
        container = _container_name(env, src)
        state = "running" if container_running(container) else "not running"
        print(f"Container {container}: {state}")


def cmd_detail(env, sources, schema_map):
    """Show detailed replication status for each source."""
    print(f"Database: {env['pg_host']}/{env['pg_db']}\n")

    if not catalogue_exists(env):
        print("No sch_chameleon schema — migration not initialised.")
        return

    def section(title):
        print("\n" + "=" * 70)
        print(title)
        print("=" * 70)

    section("SOURCE STATUS")
    psql_print(
        env,
        "SELECT s.t_source, s.enm_status, s.b_consistent, "
        "s.t_binlog_name, s.i_binlog_position, "
        "lr.ts_last_received, lp.ts_last_replayed "
        "FROM sch_chameleon.t_sources s "
        "LEFT JOIN sch_chameleon.t_last_received lr ON lr.i_id_source = s.i_id_source "
        "LEFT JOIN sch_chameleon.t_last_replayed lp ON lp.i_id_source = s.i_id_source "
        "ORDER BY s.i_id_source;",
    )

    section("RECENT BATCHES (last 10)")
    psql_print(
        env,
        "SELECT b.i_id_batch, s.t_source, "
        "b.v_log_table, b.i_replayed, b.i_skipped, b.i_ddl, "
        "b.ts_created, b.ts_replayed "
        "FROM sch_chameleon.t_replica_batch b "
        "JOIN sch_chameleon.t_sources s ON s.i_id_source = b.i_id_source "
        "ORDER BY b.i_id_batch DESC LIMIT 10;",
    )

    section("REPLICATION LAG")
    psql_print(
        env,
        "SELECT s.t_source, "
        "CASE WHEN lr.ts_last_received IS NOT NULL "
        "THEN now() - lr.ts_last_received ELSE NULL END AS receive_lag, "
        "CASE WHEN lp.ts_last_replayed IS NOT NULL "
        "THEN now() - lp.ts_last_replayed ELSE NULL END AS replay_lag "
        "FROM sch_chameleon.t_sources s "
        "LEFT JOIN sch_chameleon.t_last_received lr ON lr.i_id_source = s.i_id_source "
        "LEFT JOIN sch_chameleon.t_last_replayed lp ON lp.i_id_source = s.i_id_source "
        "ORDER BY s.i_id_source;",
    )

    # Discarded rows
    discard_count = (
        pg_query(
            env,
            "SELECT count(*) FROM sch_chameleon.t_discarded_rows;",
            single_value=True,
        )
        or "0"
    )
    print(f"\nDiscarded rows (total): {discard_count}")
    if discard_count != "0":
        psql_print(
            env,
            "SELECT i_id_batch, v_schema_name, v_table_name, ts_discard, "
            "substring(t_row_data from 1 for 100) AS data_preview "
            "FROM sch_chameleon.t_discarded_rows "
            "ORDER BY ts_discard DESC LIMIT 10;",
        )

    section("ERRORS (last 24h)")
    psql_print(
        env,
        "SELECT e.ts_error, s.t_source, e.v_schema_name, e.v_table_name, "
        "e.t_error_message "
        "FROM sch_chameleon.t_error_log e "
        "JOIN sch_chameleon.t_sources s ON s.i_id_source = e.i_id_source "
        "WHERE e.ts_error > now() - interval '24 hours' "
        "ORDER BY e.ts_error DESC LIMIT 20;",
    )

    section("CONTAINERS")
    for src in sources:
        container = _container_name(env, src)
        r = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Status}} | started {{.State.StartedAt}} | restarts {{.RestartCount}}",
                container,
            ],
            capture_output=True,
            text=True,
        )
        if r.returncode == 0:
            print(f"  {container}: {r.stdout.strip()}")
        else:
            print(f"  {container}: not found")


def cmd_logs(env, sources, schema_map, *, follow=False, lines=100):
    """Show recent logs from replica containers."""
    for src in sources:
        container = _container_name(env, src)
        print(f"\n{'='*60}")
        print(f"Logs: {container}")
        print(f"{'='*60}")
        # Check if container exists (running or stopped)
        if not container_running(container, include_stopped=True):
            print("  (no container found)")
            continue
        log_cmd = ["docker", "logs", "--tail", str(lines), "--timestamps"]
        if follow:
            log_cmd.append("--follow")
        log_cmd.append(container)
        subprocess.run(log_cmd)


def cmd_stop(env, sources, schema_map):
    for src in sources:
        print(f"Stopping {_container_name(env, src)}...")
    _stop_containers(env, sources)
    for src in sources:
        pg_exec(
            env,
            f"UPDATE sch_chameleon.t_sources SET enm_status='stopped' WHERE t_source='{_pg_source_name(src)}' AND enm_status='running';",
        )
    print("Stopped.")


def cmd_fix_sequences(env, sources, schema_map):
    print("Resetting sequences to MAX values...\n")
    for src in sources:
        schema = schema_map[src]
        print(f"  Schema: {schema}")

        # --- Pass 1: fix existing sequences that are behind ---
        sql = f"""
DO $$
DECLARE
  r RECORD;
  max_val BIGINT;
BEGIN
  FOR r IN
    SELECT s.sequencename, c.table_name, c.column_name
    FROM information_schema.columns c
    JOIN pg_sequences s
      ON s.schemaname = '{schema}'
      AND c.column_default LIKE 'nextval(%' || s.sequencename || '%'
    WHERE c.table_schema = '{schema}'
  LOOP
    EXECUTE format('SELECT COALESCE(MAX(%I), 0) FROM %I.%I',
      r.column_name, '{schema}', r.table_name) INTO max_val;
    IF max_val > 0 THEN
      PERFORM setval(format('%I.%I', '{schema}', r.sequencename), max_val);
      RAISE NOTICE 'Set %.% = %', '{schema}', r.sequencename, max_val;
    END IF;
  END LOOP;
END $$;
"""
        result = pg_exec(env, sql)
        if result.returncode != 0:
            print(f"    ERROR: {result.stderr}")
        else:
            for line in result.stderr.splitlines():
                if "NOTICE" in line:
                    print(f"    {line.strip()}")

        # --- Pass 2: create missing sequences for AUTO_INCREMENT cols ---
        ai_sql = f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{src}'
              AND EXTRA LIKE '%%auto_increment%%'
            ORDER BY TABLE_NAME, COLUMN_NAME;
        """
        ai_output = mysql_query(env, src, ai_sql)
        if not ai_output:
            continue
        for line in ai_output.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            table, col = parts
            # Check if PG column already has a sequence default
            check_sql = (
                f"SELECT column_default FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' "
                f"AND table_name = '{table}' AND column_name = '{col}';"
            )
            check = pg_exec(env, check_sql)
            col_default = check.stdout.strip() if check.returncode == 0 else ""
            if "nextval" in col_default:
                continue
            # Also skip if column doesn't exist in PG (table not migrated)
            if not col_default and col_default != "":
                continue
            seq_name = f"{table}_{col}_seq"
            create_sql = (
                f'CREATE SEQUENCE IF NOT EXISTS "{schema}"."{seq_name}";\n'
                f'SELECT setval(\'"{schema}"."{seq_name}"\', '
                f'COALESCE((SELECT MAX("{col}")::bigint '
                f'FROM "{schema}"."{table}"), 1));\n'
                f'ALTER TABLE "{schema}"."{table}" '
                f'ALTER COLUMN "{col}" '
                f'SET DEFAULT nextval(\'"{schema}"."{seq_name}"\');\n'
                f'ALTER SEQUENCE "{schema}"."{seq_name}" '
                f'OWNED BY "{schema}"."{table}"."{col}";'
            )
            r = pg_exec(env, create_sql)
            if r.returncode != 0:
                print(
                    f"    WARN creating sequence {schema}.{seq_name}: "
                    f"{r.stderr.strip()}"
                )
            else:
                print(f"    Created missing sequence: {schema}.{seq_name}")

    print("\nSequences fixed.")


def cmd_apply_fks(env, sources, schema_map):
    print("Applying foreign keys from MySQL...\n")
    total, errors = 0, 0

    for src in sources:
        schema = schema_map[src]
        print(f"  Source: {src} → schema: {schema}")

        sql = f"""
            SELECT CONCAT(
              'ALTER TABLE {schema}."', kcu.TABLE_NAME, '"',
              ' ADD CONSTRAINT "', kcu.CONSTRAINT_NAME, '"',
              ' FOREIGN KEY ("', kcu.COLUMN_NAME, '")',
              ' REFERENCES {schema}."', kcu.REFERENCED_TABLE_NAME, '"',
              '("', kcu.REFERENCED_COLUMN_NAME, '")',
              ' ON DELETE ', rc.DELETE_RULE,
              ' ON UPDATE ', rc.UPDATE_RULE, ';'
            )
            FROM information_schema.KEY_COLUMN_USAGE kcu
            JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
              ON rc.CONSTRAINT_SCHEMA = kcu.TABLE_SCHEMA
              AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            WHERE kcu.TABLE_SCHEMA = '{src}'
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME;
        """
        output = mysql_query(env, src, sql)
        if not output:
            print("    No foreign keys found.")
            continue

        stmts = [s for s in output.strip().split("\n") if s.strip()]
        print(f"    Applying {len(stmts)} FK(s)...")
        total += len(stmts)

        for stmt in stmts:
            r = pg_exec(env, stmt)
            if r.returncode != 0:
                print(f"    WARN: {stmt[:80]}...")
                errors += 1

    print(f"\n{total} FK(s) processed, {errors} error(s).")


def cmd_drop_not_nulls(env, sources, schema_map):
    """Drop NOT NULL constraints on PG columns where MySQL data contains NULLs.

    MySQL frequently has NOT NULL columns that actually contain NULL values
    (due to historical lax sql_mode). This command finds those columns and
    drops the NOT NULL constraint on the PostgreSQL side so data can flow.
    """
    print("Scanning MySQL for NOT NULL columns containing NULLs...\n")
    total_fixed = 0

    for src in sources:
        schema = schema_map[src]
        print(f"  Source: {src} → schema: {schema}")

        # Get all NOT NULL columns from MySQL (excluding auto-increment PKs)
        sql_cols = f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{src}'
              AND IS_NULLABLE = 'NO'
              AND EXTRA NOT LIKE '%%auto_increment%%'
              AND COLUMN_KEY != 'PRI'
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
        """
        output = mysql_query(env, src, sql_cols)
        if not output:
            print("    No NOT NULL columns found.")
            continue

        # Group by table
        table_cols = {}
        for line in output.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) == 2:
                table, col = parts
                table_cols.setdefault(table, []).append(col)

        for table, columns in table_cols.items():
            # Build a single query to check which columns have NULLs
            null_checks = " OR ".join([f"`{col}` IS NULL" for col in columns])
            check_sql = f"SELECT 1 FROM `{src}`.`{table}` WHERE ({null_checks}) LIMIT 1"
            has_nulls = mysql_query(env, src, check_sql)
            if not has_nulls:
                continue

            # Find exactly which columns have NULLs
            for col in columns:
                col_check = (
                    f"SELECT 1 FROM `{src}`.`{table}` WHERE `{col}` IS NULL LIMIT 1"
                )
                if mysql_query(env, src, col_check):
                    # Drop NOT NULL on the PG side
                    # Try both the target schema and the _tmp schema
                    for target_schema in [schema, f"_{schema}_tmp"]:
                        alter_sql = (
                            f'ALTER TABLE IF EXISTS "{target_schema}"."{table}" '
                            f'ALTER COLUMN "{col}" DROP NOT NULL;'
                        )
                        r = pg_exec(env, alter_sql)
                        if r.returncode == 0:
                            print(
                                f"    {target_schema}.{table}.{col}: dropped NOT NULL"
                            )
                            total_fixed += 1

    print(f"\n{total_fixed} NOT NULL constraint(s) dropped.")


def cmd_apply_not_nulls(env, sources, schema_map):
    """Re-apply NOT NULL constraints from MySQL schema to PostgreSQL (post-migration).

    This should be run after migration is complete and all data has been verified.
    It reads MySQL's schema definition and sets NOT NULL on the corresponding
    PG columns, skipping any columns that still contain NULLs in PG.

    Optimised for cutover time by working one table at a time:
      * a single aggregate query computes the NULL count for every candidate
        column in ONE table scan (instead of one scan per column), and
      * all clean columns are set NOT NULL in ONE combined ALTER TABLE, which
        PostgreSQL validates in a single scan (instead of one scan per column).
    """
    print("Applying NOT NULL constraints from MySQL schema...\n")
    total_applied, total_skipped = 0, 0

    for src in sources:
        schema = schema_map[src]
        print(f"  Source: {src} → schema: {schema}")

        # Get all NOT NULL columns from MySQL, grouped by table.
        sql_cols = f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{src}'
              AND IS_NULLABLE = 'NO'
              AND EXTRA NOT LIKE '%%auto_increment%%'
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
        """
        output = mysql_query(env, src, sql_cols)
        if not output:
            print("    No NOT NULL columns found.")
            continue

        table_cols = {}
        for line in output.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) == 2:
                table, col = parts
                table_cols.setdefault(table, []).append(col)

        for table, columns in table_cols.items():
            # One scan: count NULLs for every candidate column at once.
            filters = ", ".join(
                f'count(*) FILTER (WHERE "{col}" IS NULL)' for col in columns
            )
            counts_row = pg_query(
                env,
                f'SELECT {filters} FROM "{schema}"."{table}";',
                single_value=True,
            )
            if counts_row is None:
                # Table missing or query failed — skip quietly.
                continue
            counts = counts_row.split("|")
            if len(counts) != len(columns):
                continue

            clean, dirty = [], []
            for col, n in zip(columns, counts):
                (clean if n.strip() == "0" else dirty).append(col)

            for col in dirty:
                print(f"    skip {table}.{col} (contains NULLs)")
            total_skipped += len(dirty)

            if not clean:
                continue

            # One combined ALTER: PG validates all columns in a single scan.
            alters = ", ".join(f'ALTER COLUMN "{col}" SET NOT NULL' for col in clean)
            r = pg_exec(env, f'ALTER TABLE "{schema}"."{table}" {alters};')
            if r.returncode == 0:
                total_applied += len(clean)
                print(f"    {table}: SET NOT NULL on {len(clean)} column(s)")
            else:
                err = r.stderr.strip().splitlines()
                print(f"    WARN {table}: {err[-1] if err else 'ALTER failed'}")

    print(
        f"\n{total_applied} NOT NULL constraint(s) applied, {total_skipped} skipped (contain NULLs)."
    )


def _translate_default(mysql_default, col_type):
    """Translate a MySQL COLUMN_DEFAULT into a PostgreSQL default expression.

    Returns a SQL fragment for SET DEFAULT, or None if it can't be safely
    translated (caller skips and reports it for manual review).
    """
    val = mysql_default.strip()
    upper = val.upper()

    if upper == "NULL":
        return "NULL"
    if upper.startswith("CURRENT_TIMESTAMP") or upper in ("NOW()", "LOCALTIMESTAMP"):
        return "CURRENT_TIMESTAMP"
    # MySQL "zero dates" (0000-00-00 / 0000-00-00 00:00:00) are invalid in PG.
    # There is no valid PG equivalent, so skip them (reported for manual review).
    if ("-" in val or ":" in val) and val.strip("0:-. ") == "":
        return None
    # tinyint(1) is overridden to boolean in PG (see type_override in write_config).
    if col_type.lower() == "tinyint(1)":
        return {"0": "false", "1": "true"}.get(val.strip("b'"))
    # MySQL bit literals: b'0', b'1', b'101' → integer (PG maps bit cols to int).
    if val.lower().startswith("b'") and val.endswith("'"):
        try:
            return str(int(val[2:-1], 2))
        except ValueError:
            return None
    # Numeric literal — use verbatim; anything else is quoted as a string.
    # (MySQL 8 returns string defaults unquoted in information_schema.)
    try:
        float(val)
        return val
    except ValueError:
        return "'" + val.replace("'", "''") + "'"


def cmd_apply_defaults(env, sources, schema_map):
    """Apply MySQL column DEFAULT values to PostgreSQL (post-migration).

    pg_chameleon does not copy column default expressions during init_replica.
    This reads each column's default from MySQL information_schema and applies it
    to the matching PostgreSQL column. AUTO_INCREMENT columns are skipped (handled
    by fix-sequences) and columns already backed by a sequence default are left
    untouched. Defaults that cannot be safely translated are skipped and reported.
    """
    print("Applying column DEFAULT values from MySQL...\n")
    total_applied, skipped, errors = 0, 0, 0

    for src in sources:
        schema = schema_map[src]
        print(f"  Source: {src} \u2192 schema: {schema}")

        rows = mysql_query(
            env,
            src,
            f"""
            SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, COLUMN_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = '{src}'
              AND COLUMN_DEFAULT IS NOT NULL
              AND EXTRA NOT LIKE '%%auto_increment%%'
            ORDER BY TABLE_NAME, ORDINAL_POSITION;
            """,
        )
        if not rows:
            print("    No column defaults found.")
            continue

        for line in rows.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) < 4:
                continue
            table, col, mysql_default, col_type = parts[:4]

            # Look up the PG column. COALESCE with a sentinel lets us tell apart
            # three cases that an empty result otherwise conflates:
            #   - None            → column not in PG (table not migrated)
            #   - '__NODEFAULT__' → column exists, currently has no default (apply)
            #   - 'nextval(...)'  → column already sequence-backed (skip)
            existing = pg_query(
                env,
                f"SELECT COALESCE(column_default, '__NODEFAULT__') "
                f"FROM information_schema.columns "
                f"WHERE table_schema='{schema}' AND table_name='{table}' "
                f"AND column_name='{col}';",
                single_value=True,
            )
            if existing is None:
                # Column not found in PG (table not migrated) — nothing to do.
                continue
            if "nextval" in existing:
                skipped += 1
                continue

            pg_default = _translate_default(mysql_default, col_type)
            if pg_default is None:
                print(f"    SKIP {table}.{col}: unrecognised default '{mysql_default}'")
                skipped += 1
                continue

            alter_sql = (
                f'ALTER TABLE "{schema}"."{table}" '
                f'ALTER COLUMN "{col}" SET DEFAULT {pg_default};'
            )
            r = pg_exec(env, alter_sql)
            if r.returncode == 0:
                total_applied += 1
                print(f"    {table}.{col}: SET DEFAULT {pg_default}")
            else:
                errors += 1
                print(f"    WARN {table}.{col}: {r.stderr.strip()[:120]}")

    print(
        f"\n{total_applied} default(s) applied, {skipped} skipped, {errors} error(s)."
    )


def cmd_apply_comments(env, sources, schema_map):
    """Copy MySQL table and column COMMENTs to PostgreSQL (post-migration).

    pg_chameleon does not carry over COMMENTs during init_replica. This reads
    table and column comments from MySQL information_schema and applies them to
    the matching PostgreSQL objects.
    """
    print("Applying table and column comments from MySQL...\n")
    tbl_applied, col_applied, errors = 0, 0, 0

    for src in sources:
        schema = schema_map[src]
        print(f"  Source: {src} \u2192 schema: {schema}")

        # Table comments
        tbl_rows = mysql_query(
            env,
            src,
            f"SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA='{src}' AND TABLE_TYPE='BASE TABLE' "
            f"AND TABLE_COMMENT <> '' ORDER BY TABLE_NAME;",
        )
        if tbl_rows:
            for line in tbl_rows.strip().split("\n"):
                parts = line.split("\t")
                if len(parts) < 2:
                    continue
                table, comment = parts[0], parts[1]
                if table in SKIP_TABLE_NAMES:
                    continue
                escaped = comment.replace("'", "''")
                r = pg_exec(
                    env,
                    f'COMMENT ON TABLE "{schema}"."{table}" IS \'{escaped}\';',
                )
                if r.returncode == 0:
                    tbl_applied += 1
                else:
                    errors += 1

        # Column comments
        col_rows = mysql_query(
            env,
            src,
            f"SELECT TABLE_NAME, COLUMN_NAME, COLUMN_COMMENT "
            f"FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA='{src}' AND COLUMN_COMMENT <> '' "
            f"ORDER BY TABLE_NAME, ORDINAL_POSITION;",
        )
        if col_rows:
            for line in col_rows.strip().split("\n"):
                parts = line.split("\t")
                if len(parts) < 3:
                    continue
                table, col, comment = parts[0], parts[1], parts[2]
                if table in SKIP_TABLE_NAMES:
                    continue
                escaped = comment.replace("'", "''")
                r = pg_exec(
                    env,
                    f'COMMENT ON COLUMN "{schema}"."{table}"."{col}" '
                    f"IS '{escaped}';",
                )
                if r.returncode == 0:
                    col_applied += 1
                else:
                    errors += 1

    print(
        f"\n{tbl_applied} table comment(s), {col_applied} column comment(s) "
        f"applied, {errors} error(s)."
    )


def cmd_check_latin1(env, sources, schema_map):
    """Report latin1 columns that may suffer decode corruption during replication.

    pg_chameleon's decode-error patch silently replaces non-UTF-8 bytes with the
    Unicode replacement character (\ufffd). This command finds all latin1 columns,
    checks which ones actually contain non-ASCII bytes, and prints the diff-rows.py
    commands needed to validate those tables after migration.
    """
    print(f"Scanning for latin1 columns that may corrupt during replication...\n")

    all_risk_tables = []  # (src, schema, table) tuples needing diff validation

    for src in sources:
        schema = schema_map[src]
        print(f"Source: {src} \u2192 schema: {schema}")
        print("-" * 60)

        # Get all latin1 columns grouped by table
        col_output = mysql_query(
            env,
            src,
            f"SELECT TABLE_NAME, COLUMN_NAME "
            f"FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA = '{src}' "
            f"AND CHARACTER_SET_NAME = 'latin1' "
            f"ORDER BY TABLE_NAME, COLUMN_NAME;",
        )
        if not col_output or not col_output.strip():
            print("  OK: no latin1 columns found.\n")
            continue

        # Group by table
        table_cols: dict = {}
        for line in col_output.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) == 2:
                table_cols.setdefault(parts[0].strip(), []).append(parts[1].strip())

        print(f"  {len(table_cols)} table(s) with latin1 column(s):")

        for table, cols in sorted(table_cols.items()):
            col_list = ", ".join(cols)
            # Check if any of these columns actually contain bytes > 0x7F
            # (pure ASCII latin1 is safe; only extended characters corrupt)
            hex_checks = " OR ".join(
                [f"HEX(`{c}`) REGEXP '[89A-Fa-f][0-9A-Fa-f]'" for c in cols]
            )
            has_nonascii = mysql_query(
                env,
                src,
                f"SELECT 1 FROM `{src}`.`{table}` WHERE {hex_checks} LIMIT 1;",
            )
            if has_nonascii and has_nonascii.strip():
                print(f"  RISK  {table}: {col_list}")
                print(
                    f"        ^^^ contains non-ASCII bytes — WILL corrupt on replication"
                )
                all_risk_tables.append((src, schema, table))
            else:
                print(f"  OK    {table}: {col_list} (latin1 but all-ASCII)")
        print()

    # Print diff-rows.py commands for all risk tables
    if all_risk_tables:
        print("=" * 60)
        print("Tables requiring diff-rows.py validation after migration:")
        print("=" * 60)
        for src, schema, table in all_risk_tables:
            print(
                f"python3 diff-rows.py --env-file <your.env> "
                f"--mysql-db {src} --schema {schema} --table {table}"
            )
    else:
        print(
            "No tables contain non-ASCII latin1 data — decode corruption risk is zero."
        )


def cmd_validate(env, sources, schema_map):
    """Compare row counts between MySQL and PostgreSQL for all replicated tables."""
    print(f"Database: {env['pg_host']}/{env['pg_db']}\n")

    issues = []
    ok_count = 0

    for src in sources:
        schema = schema_map[src]
        print(f"Source: {src} → schema: {schema}")
        print("-" * 60)

        # Get table list from MySQL
        tables_raw = mysql_query(
            env,
            src,
            f"SELECT TABLE_NAME FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA='{src}' AND TABLE_TYPE='BASE TABLE' "
            f"ORDER BY TABLE_NAME",
        )
        if not tables_raw:
            print("  Could not get table list from MySQL.")
            continue

        tables = [t.strip() for t in tables_raw.split("\n") if t.strip()]

        for table in tables:
            if table in SKIP_TABLE_NAMES:
                print(f"  {table}: skipped")
                continue

            mc = mysql_query(env, src, f"SELECT COUNT(*) FROM `{src}`.`{table}`")
            pc = pg_query(
                env,
                f'SELECT COUNT(*) FROM "{schema}"."{table}"',
                single_value=True,
            )

            if mc is None:
                issues.append(f"MySQL error: {src}.{table}")
                print(f"  {table}: MySQL query error")
                continue
            if pc is None:
                issues.append(f"PG missing: {schema}.{table}")
                print(f"  {table}: not found in PostgreSQL")
                continue

            mysql_n, pg_n = int(mc), int(pc)
            diff = mysql_n - pg_n

            if diff == 0:
                ok_count += 1
                print(f"  {table}: OK ({mysql_n:,})")
            else:
                pct = (abs(diff) / max(mysql_n, 1)) * 100
                issues.append(
                    f"Row count mismatch: {table} (MySQL={mysql_n:,} PG={pg_n:,})"
                )
                print(
                    f"  {table}: MISMATCH  MySQL={mysql_n:,}  PG={pg_n:,}  "
                    f"diff={diff:+,} ({pct:.4f}%)"
                )

        print()

    # Catalogue diagnostics
    if catalogue_exists(env):
        # Pending batches (not yet replayed)
        pending = (
            pg_query(
                env,
                "SELECT count(*) FROM sch_chameleon.t_replica_batch "
                "WHERE NOT b_processed AND NOT b_replayed;",
                single_value=True,
            )
            or "0"
        )
        if pending != "0":
            print(f"Pending batches (not yet replayed): {pending}")
            print("  Row mismatches may decrease once pending batches are replayed.\n")

        discard_count = (
            pg_query(
                env,
                "SELECT count(*) FROM sch_chameleon.t_discarded_rows;",
                single_value=True,
            )
            or "0"
        )
        error_count = (
            pg_query(
                env,
                "SELECT count(*) FROM sch_chameleon.t_error_log;",
                single_value=True,
            )
            or "0"
        )

        print(
            f"Catalogue: {discard_count} discarded row(s), {error_count} error(s) total"
        )

        if int(discard_count) > 0:
            print("\nDiscarded rows by table:")
            psql_print(
                env,
                "SELECT v_schema_name, v_table_name, count(*) AS discarded "
                "FROM sch_chameleon.t_discarded_rows "
                "GROUP BY v_schema_name, v_table_name "
                "ORDER BY count(*) DESC LIMIT 20;",
            )

        if int(error_count) > 0:
            print("\nRecent errors (last 20):")
            psql_print(
                env,
                "SELECT e.ts_error, s.t_source, e.v_table_name, "
                "substring(e.t_error_message from 1 for 150) AS error "
                "FROM sch_chameleon.t_error_log e "
                "JOIN sch_chameleon.t_sources s ON s.i_id_source = e.i_id_source "
                "ORDER BY e.ts_error DESC LIMIT 20;",
            )

    # Summary
    print(f"\n{'='*60}")
    print(f"Summary: {len(issues)} issue(s) found, {ok_count} table(s) OK\n")
    for i, issue in enumerate(issues, 1):
        print(f"  {i:3d}. {issue}")
    if not issues:
        print("  All tables match!")


def cmd_diagnose(env, sources, schema_map):
    """Pre-flight checks for MySQL→PG replication: binlog, grants, GTID, connectivity."""
    print(f"Diagnosing replication prerequisites...\n")
    warnings = []
    errors = []

    # ── MySQL connectivity ──
    print("1. MySQL connectivity")
    r = mysql_query(env, sources[0], "SELECT 1")
    if r is None:
        errors.append("Cannot connect to MySQL")
        print("   FAIL: Cannot connect to MySQL")
    else:
        print("   OK")

    # ── PostgreSQL connectivity ──
    print("2. PostgreSQL connectivity")
    r = pg_query(env, "SELECT 1", single_value=True)
    if r is None:
        errors.append("Cannot connect to PostgreSQL")
        print("   FAIL: Cannot connect to PostgreSQL")
    else:
        print("   OK")

    if errors:
        print(f"\n{len(errors)} connectivity error(s) — fix these first.")
        return

    # ── MySQL version ──
    print("3. MySQL version")
    version = mysql_query(env, sources[0], "SELECT @@version")
    if version:
        print(f"   {version}")
        # Warn about MySQL 8.4+ SHOW MASTER STATUS deprecation
        parts = version.split(".")
        try:
            major, minor = int(parts[0]), int(parts[1])
            if major > 8 or (major == 8 and minor >= 4):
                warnings.append(
                    f"MySQL {version}: SHOW MASTER STATUS is deprecated. "
                    "pg_chameleon may need patching (issue #194). "
                    "The diagnose command already uses SHOW BINARY LOG STATUS as fallback."
                )
                print(f"   WARN: MySQL 8.4+ deprecates SHOW MASTER STATUS")
        except (ValueError, IndexError):
            pass
    else:
        warnings.append("Could not determine MySQL version")
        print("   WARN: Could not determine MySQL version")

    # ── Binary logging ──
    print("4. MySQL binary logging")
    log_bin = mysql_query(env, sources[0], "SELECT @@log_bin")
    if log_bin and log_bin.strip() in ("1", "ON"):
        print("   OK: log_bin is ON")
    else:
        errors.append("Binary logging (log_bin) is OFF — replication cannot work")
        print(f"   FAIL: log_bin = {log_bin}")

    # ── Binlog format ──
    print("5. MySQL binlog format")
    fmt = mysql_query(env, sources[0], "SELECT @@binlog_format")
    if fmt and fmt.strip().upper() == "ROW":
        print("   OK: binlog_format = ROW")
    else:
        errors.append(f"binlog_format is '{fmt}' — must be ROW for pg_chameleon")
        print(f"   FAIL: binlog_format = {fmt} (must be ROW)")

    # ── Binlog row image ──
    print("6. MySQL binlog row image")
    img = mysql_query(env, sources[0], "SELECT @@binlog_row_image")
    if img and img.strip().upper() == "FULL":
        print("   OK: binlog_row_image = FULL")
    else:
        warnings.append(f"binlog_row_image is '{img}' — FULL is recommended")
        print(f"   WARN: binlog_row_image = {img} (FULL recommended)")

    # ── GTID mode ──
    print("7. MySQL GTID mode")
    gtid = mysql_query(env, sources[0], "SELECT @@gtid_mode")
    gtid_val = gtid.strip().upper() if gtid else "OFF"
    gtid_cfg = os.environ.get("CHAMELEON_GTID_ENABLE", "true").lower() in (
        "true",
        "1",
        "yes",
    )
    if gtid_val == "ON" and not gtid_cfg:
        errors.append(
            "MySQL has GTID mode ON but CHAMELEON_GTID_ENABLE is not set — "
            "init_replica will not capture binlog position! "
            "Set CHAMELEON_GTID_ENABLE=true in your env."
        )
        print(f"   FAIL: gtid_mode = {gtid_val} but CHAMELEON_GTID_ENABLE is false")
        print("         Set CHAMELEON_GTID_ENABLE=true in your migration.env")
    elif gtid_val == "ON" and gtid_cfg:
        print(f"   OK: gtid_mode = ON, CHAMELEON_GTID_ENABLE = true")
    elif gtid_val == "OFF" and gtid_cfg:
        warnings.append("CHAMELEON_GTID_ENABLE is true but MySQL GTID is OFF")
        print(f"   WARN: CHAMELEON_GTID_ENABLE is true but gtid_mode is OFF")
    else:
        print(f"   OK: gtid_mode = {gtid_val}, CHAMELEON_GTID_ENABLE = false")

    # ── SHOW MASTER STATUS ──
    # MySQL 8.4+ deprecated SHOW MASTER STATUS in favour of SHOW BINARY LOG STATUS
    print("8. MySQL binlog position")
    master = mysql_query(env, sources[0], "SHOW BINARY LOG STATUS")
    if master is None:
        # Fallback for MySQL < 8.4
        master = mysql_query(env, sources[0], "SHOW MASTER STATUS")
    if master and master.strip():
        parts = master.split("\t")
        print(f"   OK: file={parts[0]}, position={parts[1] if len(parts) > 1 else '?'}")
    else:
        errors.append(
            "SHOW MASTER STATUS returned empty — the MySQL user likely lacks "
            "REPLICATION CLIENT privilege"
        )
        print("   FAIL: empty result — user needs REPLICATION CLIENT grant")

    # ── Replication grants ──
    print("9. MySQL user grants")
    grants = mysql_query(env, sources[0], "SHOW GRANTS FOR CURRENT_USER()")
    if grants:
        grants_upper = grants.upper()
        has_repl_slave = "REPLICATION SLAVE" in grants_upper
        has_repl_client = "REPLICATION CLIENT" in grants_upper
        has_all = "ALL PRIVILEGES" in grants_upper
        if has_all or (has_repl_slave and has_repl_client):
            print("   OK: REPLICATION SLAVE + REPLICATION CLIENT present")
        else:
            if not has_repl_slave:
                errors.append("MySQL user missing REPLICATION SLAVE grant")
                print("   FAIL: missing REPLICATION SLAVE")
            if not has_repl_client:
                errors.append("MySQL user missing REPLICATION CLIENT grant")
                print("   FAIL: missing REPLICATION CLIENT")
        # Print grants for reference
        for line in grants.split("\n"):
            if line.strip():
                print(f"     {line.strip()}")
    else:
        warnings.append("Could not retrieve grants")
        print("   WARN: Could not retrieve grants")

    # ── Catalogue binlog position (if already initialized) ──
    print("10. Catalogue binlog position")
    if catalogue_exists(env):
        for src in sources:
            if source_registered(env, _pg_source_name(src)):
                row = pg_query(
                    env,
                    f"SELECT t_binlog_name, i_binlog_position, enm_status, b_consistent "
                    f"FROM sch_chameleon.t_sources WHERE t_source='{_pg_source_name(src)}';",
                )
                if not row:
                    errors.append(f"Could not read t_sources for '{src}'")
                    print(f"   FAIL: could not read catalogue for '{src}'")
                    continue
                parts = row.strip().split("|")
                binlog = parts[0].strip() if len(parts) > 0 else ""
                binpos = parts[1].strip() if len(parts) > 1 else ""
                src_status = parts[2].strip() if len(parts) > 2 else ""
                consistent = parts[3].strip() if len(parts) > 3 else ""
                if not binlog:
                    # Empty binlog/pos is NORMAL with GTID once b_consistent=t.
                    # pg_chameleon clears the file/offset coords after reaching a
                    # consistent state — position is tracked via the GTID set instead.
                    # This applies whether the source is 'running' or 'stopped'.
                    if consistent == "t":
                        print(
                            f"   OK: source '{src}' binlog position is empty — "
                            "normal with GTID after replica reaches consistent state"
                        )
                    else:
                        errors.append(
                            f"Source '{src}' has EMPTY binlog position "
                            f"(status={src_status}, consistent={consistent}) — "
                            "init_replica did not capture binlog coordinates. "
                            "Check GTID/grant settings and reset."
                        )
                        print(f"   FAIL: source '{src}' binlog_name is EMPTY")
                        print(f"         status={src_status}, consistent={consistent}")
                        print("         Fix grants/GTID settings, then: reset → start")
                else:
                    print(f"   OK: source '{src}' binlog={binlog} pos={binpos}")
            else:
                print(f"   SKIP: source '{src}' not registered yet")
    else:
        print("   SKIP: catalogue not created yet")

    # ── on_error_replay setting ──
    print("11. Error handling policy")
    err_replay = os.environ.get("CHAMELEON_ON_ERROR_REPLAY", "exit")
    err_read = os.environ.get("CHAMELEON_ON_ERROR_READ", "exit")
    if err_replay == "continue":
        warnings.append(
            "on_error_replay=continue — replay errors are silently skipped. "
            "Set CHAMELEON_ON_ERROR_REPLAY=exit to catch failures."
        )
        print(f"   WARN: on_error_replay=continue (errors silently skipped)")
        print("         Set CHAMELEON_ON_ERROR_REPLAY=exit to detect failures")
    else:
        print(f"   OK: on_error_replay={err_replay}")
    print(f"   on_error_read={err_read}")

    # ── Tables without primary keys ──
    print("12. Tables without primary/unique keys")
    for src in sources:
        no_pk = mysql_query(
            env,
            src,
            f"SELECT GROUP_CONCAT(TABLE_NAME SEPARATOR ', ') FROM information_schema.TABLES t "
            f"WHERE t.TABLE_SCHEMA = '{src}' AND t.TABLE_TYPE = 'BASE TABLE' "
            f"AND NOT EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS tc "
            f"WHERE tc.TABLE_SCHEMA = t.TABLE_SCHEMA AND tc.TABLE_NAME = t.TABLE_NAME "
            f"AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE'))",
        )
        if no_pk and no_pk.strip() and no_pk.strip().upper() != "NULL":
            warnings.append(
                f"Source '{src}' has tables without PK/unique key: {no_pk.strip()}. "
                "These will be initialised but NOT replicated."
            )
            print(f"   WARN [{src}]: {no_pk.strip()}")
            print("         These tables will NOT receive replica updates.")
        else:
            print(f"   OK [{src}]: all tables have primary/unique keys")

    # ── Summary ──
    print(f"\n{'='*60}")
    print(f"Diagnosis: {len(errors)} error(s), {len(warnings)} warning(s)")
    print(f"{'='*60}")
    if errors:
        print("\nERRORS (must fix):")
        for i, e in enumerate(errors, 1):
            print(f"  {i}. {e}")
    if warnings:
        print("\nWARNINGS:")
        for i, w in enumerate(warnings, 1):
            print(f"  {i}. {w}")
    if not errors and not warnings:
        print("\nAll checks passed — replication prerequisites look good.")
    elif errors:
        print("\nFix the errors above before running start/init_replica.")


def cmd_reset(env, sources, schema_map):
    if not catalogue_exists(env):
        sys.exit("sch_chameleon does not exist — nothing to reset.")

    schemas = [schema_map[s] for s in sources]
    print(f"WARNING: This will destroy schemas on {env['pg_host']}/{env['pg_db']}:")
    for s in schemas:
        print(f"  - {s}")
    print("  - sch_chameleon\n")

    if sys.stdin.isatty():
        confirm = input("Type 'yes' to confirm: ")
        if confirm != "yes":
            sys.exit("Aborted.")

    _stop_containers(env, sources)
    pg_exec(env, "DROP SCHEMA IF EXISTS sch_chameleon CASCADE;")
    for src in sources:
        schema = schema_map[src]
        pg_exec(env, f"DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema};")

    print("Reset complete.")


# ─── Argument parsing & env loading ──────────────────────────────────────────


def load_env(args):
    """Build env dict from env-file, environment, and CLI flags."""
    # Load env file if specified
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

    # Validate
    missing = [k for k, v in env.items() if not v]
    if missing:
        sys.exit(f"Missing: {', '.join(missing)}")

    return env


def parse_sources(args):
    """Parse --mysql-db into list and --schema into target schema."""
    db_str = args.mysql_db or os.environ.get("MYSQL_DB", "")
    if not db_str:
        sys.exit("No databases specified (--mysql-db or MYSQL_DB)")

    sources = [d.strip() for d in db_str.split(",")]
    target_schema = args.schema or os.environ.get("PG_SCHEMA", "public")
    schema_map = {db: target_schema for db in sources}

    # Filter by --source
    if args.source:
        if args.source not in sources:
            sys.exit(f"Source '{args.source}' not in: {sources}")
        sources = [args.source]

    return sources, schema_map


def main():
    parser = argparse.ArgumentParser(description="pg_chameleon migration operator")
    parser.add_argument(
        "command",
        choices=[
            "start",
            "status",
            "detail",
            "logs",
            "stop",
            "fix-sequences",
            "apply-fks",
            "drop-not-nulls",
            "apply-not-nulls",
            "apply-defaults",
            "apply-comments",
            "validate",
            "diagnose",
            "check-latin1",
            "reset",
        ],
    )
    parser.add_argument("--mysql-host")
    parser.add_argument("--mysql-user")
    parser.add_argument("--mysql-db")
    parser.add_argument("--mysql-pass-file")
    parser.add_argument("--pg-host")
    parser.add_argument("--pg-user")
    parser.add_argument("--pg-db")
    parser.add_argument("--pg-pass-file")
    parser.add_argument("--schema", help="Target PostgreSQL schema (default: public)")
    parser.add_argument("--source")
    parser.add_argument("--env-file")
    parser.add_argument(
        "--no-shred-passwords",
        action="store_true",
        help="Keep password files after command completes (default: shred)",
    )
    parser.add_argument(
        "--follow",
        "-f",
        action="store_true",
        help="Follow log output (logs command only)",
    )
    parser.add_argument(
        "--lines",
        "-n",
        type=int,
        default=100,
        help="Number of log lines to show (logs command only, default: 100)",
    )
    args = parser.parse_args()

    env = load_env(args)
    sources, schema_map = parse_sources(args)

    t0 = time.time()

    commands = {
        "start": cmd_start,
        "status": cmd_status,
        "detail": cmd_detail,
        "logs": cmd_logs,
        "stop": cmd_stop,
        "fix-sequences": cmd_fix_sequences,
        "apply-fks": cmd_apply_fks,
        "drop-not-nulls": cmd_drop_not_nulls,
        "apply-not-nulls": cmd_apply_not_nulls,
        "apply-defaults": cmd_apply_defaults,
        "apply-comments": cmd_apply_comments,
        "validate": cmd_validate,
        "diagnose": cmd_diagnose,
        "check-latin1": cmd_check_latin1,
        "reset": cmd_reset,
    }
    if args.command == "logs":
        cmd_logs(env, sources, schema_map, follow=args.follow, lines=args.lines)
    else:
        commands[args.command](env, sources, schema_map)

    # Shred password files unless explicitly told not to
    if not args.no_shred_passwords:
        print("\nCleaning up password files...")
        shred_password_files()

    elapsed = int(time.time() - t0)
    print(f"\nTime: {elapsed // 3600}h {elapsed % 3600 // 60}m {elapsed % 60}s")


if __name__ == "__main__":
    main()

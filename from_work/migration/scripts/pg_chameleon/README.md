# Python pg_chameleon operator

Replaces the shell-based `run-chameleon-migrate.sh` with a single Python script and a cleaned-up Docker image.

## Files

| File                       | Purpose                                                                                                                                                                                   |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chameleon.py`             | Operator CLI: start, status, detail, logs, stop, diagnose, validate, fix-sequences, apply-fks, drop-not-nulls, apply-not-nulls, apply-defaults, apply-comments, check-latin1, reset       |
| `compare-mysql-pg.sh`      | Standalone comparison: inventory, row counts, column types, PK fingerprints, sequences, FKs; supports `--fast`, `--quiet`, `--zero-dates`                                                 |
| `diff-rows.py`             | Find exact missing/extra rows by PK; bucketed diff for large integer-PK tables; latin1 corruption scan; supports `--csv` export                                                           |
| `compare-mysql-pg-data.py` | Actual row-data comparison: streams rows from both databases, hashes each row, and reports mysql-only, pg-only, and changed rows per table; supports `--inspect N` for column-level diffs |
| `patch-pg-chameleon.py`    | Combined patch (Azure SSL, decode errors, index name collisions, unsigned int promotion, skip NOT NULL, skip SAVEPOINT, MySQL 8.4 binlog status)                                          |
| `Dockerfile.chameleon`     | Builds the patched pg_chameleon Docker image (pins mysql-replication<1.0)                                                                                                                 |
| `migration.env.example`    | Example env file with all connection/tuning variables                                                                                                                                     |

---

## VM setup (fresh deploy)

### 1. Prerequisites (apt)

```bash
sudo apt-get update -qq
sudo apt-get install -y -qq \
  docker-ce docker-ce-cli containerd.io \
  mysql-client postgresql-client-16 \
  python3.11 python3-pip python3.11-venv \
  jq htop
sudo usermod -aG docker "$USER" && newgrp docker
```

> If Terraform provisioned the VM, these are already present via `bootstrap.sh.tftpl`.

### 2. Azure CLI

```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az login --identity          # uses the VM's managed identity
az account set -s <SUB_ID>   # target subscription
```

### 3. Docker memory config

On low-memory workers (≤2GB), set the default runtime memory limit:

```bash
# /etc/docker/daemon.json
{ "default-ulimits": { "memlock": { "Name": "memlock", "Soft": -1, "Hard": -1 } } }
```

```bash
sudo systemctl restart docker
```

### 4. Build & deploy the image

> **Note:** `--platform linux/amd64` is required when building on Apple Silicon (M1/M2/M3)
> since the target VM runs x86_64 Linux. On an Intel/AMD machine you can omit it.

**Linux/macOS:**

```bash
# Build locally (or on the VM itself)
cd migration/scripts/pg_chameleon
docker build --platform linux/amd64 -f Dockerfile.chameleon -t pg-chameleon:latest .

# Transfer to remote VM via az ssh
docker save pg-chameleon:latest | gzip > pg-chameleon.tar.gz
cat pg-chameleon.tar.gz | az ssh vm -g <RG> -n <VM> --prefer-private-ip -- "docker load"
```

**Windows (PowerShell):**

```powershell
# Build locally (Windows is typically x86_64 — no --platform flag needed)
cd migration\scripts\pg_chameleon
docker build -f Dockerfile.chameleon -t pg-chameleon:latest .

# Transfer to remote VM via az ssh
docker save pg-chameleon:latest | gzip > pg-chameleon.tar.gz
Get-Content pg-chameleon.tar.gz -Raw | az ssh vm -g <RG> -n <VM> --prefer-private-ip -- "docker load"
```

### 5. Deploy scripts to the VM

```bash
vm() { az ssh vm -g rg-jumpboxaccess-prd-we-001 -n vm-jumpboxaccess-prd-we-001 --prefer-private-ip -- "$@"; }
vm "mkdir -p ~/migration/scripts/pg_chameleon"
for f in chameleon.py compare-mysql-pg.sh diff-rows.py patch-pg-chameleon.py Dockerfile.chameleon migration.env.example README.md; do
  vm "cat > ~/migration/scripts/pg_chameleon/$f" < "$f"
done
```

### 6. Env file & secrets

Copy and edit the example env file:

```bash
cd migration/scripts/pg_chameleon/
cp migration-secrets.env.example migration-secrets.env
chmod 600 migration-secrets.env
# Edit with your connection details (MYSQL_FQDN, PG_FQDN, MYSQL_USER, etc.)
```

Write password files (interactive, no secrets in shell history):

```bash
bash migration/scripts/bash/write-passwords.sh
```

This creates `~/.mysql_migration_pw` and `~/.pg_migration_pw` (mode 0600).
`chameleon.py` reads these automatically when `MYSQL_PASSWORD` / `PG_PASSWORD` env vars are unset.

---

## Tuning

All variables are optional. The values shown are the defaults used when a variable is not set.

### Memory

| Env var                     | Default | Notes                                                                          |
| --------------------------- | ------- | ------------------------------------------------------------------------------ |
| `CHAMELEON_COPY_MAX_MEMORY` | `300M`  | RAM per table during `init_replica`; Python RSS ≈ 3–5× this value              |
| `CHAMELEON_MEMORY_LIMIT`    | `1g`    | Docker `--memory` cap; must be **> 2×** `COPY_MAX_MEMORY`; exit 137 = OOM kill |

> `chameleon.py start` warns at startup if `CHAMELEON_MEMORY_LIMIT < 2 × CHAMELEON_COPY_MAX_MEMORY`.

### Replication behaviour

| Env var                        | Default    | Notes                                                                  |
| ------------------------------ | ---------- | ---------------------------------------------------------------------- |
| `CHAMELEON_REPLICA_BATCH_SIZE` | `10000`    | Rows per batch during read                                             |
| `CHAMELEON_REPLAY_MAX_ROWS`    | `10000`    | Rows per batch during replay                                           |
| `CHAMELEON_BATCH_RETENTION`    | `3 days`   | How long to keep batch data for debugging                              |
| `CHAMELEON_COPY_MODE`          | `file`     | `file` (default) or `direct`                                           |
| `CHAMELEON_GTID_ENABLE`        | `true`     | Set to `true` when MySQL has `gtid_mode=ON` (required for Azure MySQL) |
| `CHAMELEON_ON_ERROR_REPLAY`    | `exit`     | `exit` (safe — stops on error) or `continue` (silently skips errors)   |
| `CHAMELEON_ON_ERROR_READ`      | `exit`     | Same as above for the read side                                        |
| `CHAMELEON_AUTO_MAINTENANCE`   | `disabled` | PostgreSQL interval to trigger auto-maintenance, e.g. `1 day`          |
| `CHAMELEON_LOG_LEVEL`          | `info`     | `debug`, `info`, `warning`, `error`                                    |

> **Important:** `on_error_replay` and `on_error_read` default to `exit`. The old default was `continue`, which silently skips replication errors and loses rows without any warning. Do not set `continue` in production.

### MySQL session timeouts

| Env var                       | Default | Notes                                       |
| ----------------------------- | ------- | ------------------------------------------- |
| `CHAMELEON_NET_READ_TIMEOUT`  | `3600`  | MySQL session `net_read_timeout` (seconds)  |
| `CHAMELEON_NET_WRITE_TIMEOUT` | `3600`  | MySQL session `net_write_timeout` (seconds) |
| `CHAMELEON_WAIT_TIMEOUT`      | `28800` | MySQL session `wait_timeout` (seconds)      |

Azure MySQL Flexible Server may also need server-side parameter changes via Portal or CLI:

```bash
az mysql flexible-server parameter set \
  -g <RG> -s <SERVER> -n net_read_timeout --value 3600
az mysql flexible-server parameter set \
  -g <RG> -s <SERVER> -n wait_timeout --value 28800
```

---

## Usage

An example env file is at [`migration.env`](migration.env) — copy and fill in your values:

```bash
cp migration.env.example && chmod 600 migration.env
```

### Basic (single database → `public` schema)

```bash
python3 chameleon.py start  --env-file migration.env
python3 chameleon.py status --env-file migration.env
python3 chameleon.py stop   --env-file migration.env
```

### Target a named schema

Use `--schema` to place tables in a specific PostgreSQL schema (default: `public`):

```bash
python3 chameleon.py start --env-file migration.env \
  --mysql-db vz_licenses --schema desktop
```

Or set `PG_SCHEMA=desktop` in your env file.

### Monitoring

```bash
# Quick overview: source status, error count, container state
python3 chameleon.py status --env-file migration.env

# Detailed view: batch stats, replication lag, discarded rows, full errors, container uptime
python3 chameleon.py detail --env-file migration.env

# Raw container logs (last 100 lines per source)
python3 chameleon.py logs --env-file migration.env

# Follow logs in real time
python3 chameleon.py logs --env-file migration.env --follow

# Show last 500 lines
python3 chameleon.py logs --env-file migration.env --lines 500
```

### Validation

Compare row counts between MySQL and PostgreSQL to find mismatches. Also reports discarded rows and errors from the pg_chameleon catalogue.

```bash
python3 chameleon.py validate --env-file migration.env

# Validate a specific source only
python3 chameleon.py validate --env-file migration.env --source parallels
```

### Pre-flight diagnosis

Before starting replication, run `diagnose` to check all prerequisites:

```bash
python3 chameleon.py diagnose --env-file migration.env
```

Checks performed (in order):

1. MySQL connectivity
2. PostgreSQL connectivity
3. MySQL version (warns if 8.4+ where `SHOW MASTER STATUS` is deprecated)
4. Binary logging enabled (`log_bin=ON`)
5. `binlog_format=ROW`
6. `binlog_row_image=FULL` (recommended)
7. GTID mode vs `CHAMELEON_GTID_ENABLE` setting
8. MySQL user grants (`REPLICATION CLIENT`, `REPLICATION SLAVE`, `SELECT`)
9. PostgreSQL user grants (`CREATE`, `CONNECT`, schema privileges)
10. Binlog position readable — verifies a position was captured after `init_replica`
11. `on_error_replay` — warns if set to `continue`
12. Tables without primary or unique keys (these will **not** be replicated)

Exits with a summary of errors (must fix before running `start`) and warnings.

### NOT NULL handling

MySQL tables may have NOT NULL columns that actually contain NULL values (due to strict-mode differences). These commands manage constraints safely:

```bash
# Drop NOT NULL on columns where MySQL data already has NULLs
python3 chameleon.py drop-not-nulls --env-file migration.env

# Re-apply NOT NULL after migration (skips columns that still have NULLs in PG)
# Optimised: checks all columns for a table in one scan, then applies all clean
# columns in a single ALTER TABLE (one PG scan per table, not one per column)
python3 chameleon.py apply-not-nulls --env-file migration.env
```

### Post-migration schema cleanup

pg_chameleon copies data but does not carry over all schema metadata. These commands fill the gaps:

```bash
# Sync sequences to MAX(id) + create missing sequences for AUTO_INCREMENT columns
python3 chameleon.py fix-sequences --env-file migration.env

# Re-apply foreign keys from MySQL
python3 chameleon.py apply-fks --env-file migration.env

# Copy MySQL column DEFAULT values to PostgreSQL
# AUTO_INCREMENT columns and sequence-backed columns are skipped automatically
python3 chameleon.py apply-defaults --env-file migration.env

# Copy MySQL table and column COMMENTs to PostgreSQL
python3 chameleon.py apply-comments --env-file migration.env
```

`fix-sequences` runs two passes:

1. **Existing sequences** — sets each sequence's value to `MAX(column)` so inserts get the next correct ID.
2. **Missing sequences** — finds MySQL AUTO_INCREMENT columns with no corresponding PG sequence, creates the sequence, sets the default, and wires up ownership.

`apply-defaults` translates MySQL default expressions to their PostgreSQL equivalents:

| MySQL default        | PostgreSQL default  | Notes                                     |
| -------------------- | ------------------- | ----------------------------------------- |
| `CURRENT_TIMESTAMP`  | `CURRENT_TIMESTAMP` |                                           |
| `0000-00-00`         | _(skipped)_         | Invalid in PG; reported for manual review |
| `b'1'` (bit)         | `1`                 | Converted to integer                      |
| `tinyint(1)` `0`/`1` | `false`/`true`      | Maps to boolean                           |
| Numeric literal      | Verbatim            |                                           |
| String value         | `'value'`           | Single-quoted and escaped                 |

### Latin1 encoding scan

pg_chameleon's decode-error patch silently replaces non-UTF-8 bytes with the Unicode replacement character (`U+FFFD`). Run `check-latin1` **before** migration to identify tables at risk:

```bash
python3 chameleon.py check-latin1 --env-file migration.env
```

Output per table:

- **RISK** — latin1 columns contain bytes > 0x7F; these will be corrupted during replication
- **OK** — latin1 columns found but all values are pure ASCII (safe)

The command also prints the exact `diff-rows.py --check-latin1` invocations needed to validate RISK tables after migration.

### Reset (start fresh)

```bash
python3 chameleon.py reset --env-file migration.env
```

Drops the `sch_chameleon` catalogue and target schemas, allowing a clean re-run.

### Row-level diff

After `validate` shows row count mismatches, use `diff-rows.py` to find the exact primary-key values that are missing or extra. This is a standalone script (not part of the Docker image) that runs directly on the VM or locally:

```bash
# Diff a specific table
python3 diff-rows.py --env-file migration.env --table domain

# Diff multiple tables at once
python3 diff-rows.py --env-file migration.env --table domain,subscription

# Auto-scan all tables and diff only those with mismatches
python3 diff-rows.py --env-file migration.env

# Dump sample row data from MySQL for missing rows
python3 diff-rows.py --env-file migration.env --table domain --dump 5

# Write missing PKs to a file for re-sync
python3 diff-rows.py --env-file migration.env --table domain --output missing-pks.txt

# Export full row data for missing rows to CSV files
python3 diff-rows.py --env-file migration.env --table domain --csv ./exports
```

#### Bucketed diff for large tables

For tables with a single integer primary key, `diff-rows.py` uses a **bucket-based algorithm** that avoids transferring the full PK set over the network:

1. Fetches `MIN`/`MAX` PK from both sides (two fast index scans).
2. Compares row counts per equal-width bucket via `GROUP BY` — one query each side.
3. Fetches actual PKs only for buckets where the counts differ.

For a 17M-row table with ~300 differences this completes in seconds rather than streaming 17M rows.

```bash
# Adjust bucket width (default: 50000 rows)
# Smaller = more precise but more queries; larger = fewer queries but coarser
python3 diff-rows.py --env-file migration.env --table domain --bucket-size 10000
```

#### Latin1 corruption scan

`diff-rows.py` can scan PostgreSQL for rows where latin1 columns contain `U+FFFD` (the Unicode replacement character), which indicates silent corruption by the decode-error patch:

```bash
python3 diff-rows.py --env-file migration.env --table contacts --check-latin1
```

### Data-level row comparison

`compare-mysql-pg-data.py` goes beyond row counts: it hashes every row in both databases and streams a merge to identify rows that are mysql-only, pg-only, or present on both sides but with changed content. Use this after `validate` shows a discrepancy, or as a final pre-cutover check.

```bash
# Compare all tables in a named schema
python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras

# Compare specific tables only
python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \
    --table farm,account

# Show column-level diffs for the first 5 rows of each diff type
python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \
    --inspect 5

# Focus on a single table with detailed inspection
python3 compare-mysql-pg-data.py --env-file migration.env --pg-schema ras \
    --table farm --inspect 10
```

Output columns per table:

| Column       | Meaning                                                       |
| ------------ | ------------------------------------------------------------- |
| `MYSQL_ONLY` | Rows in MySQL with no matching PK in PostgreSQL               |
| `PG_ONLY`    | Rows in PostgreSQL with no matching PK in MySQL               |
| `CHANGED`    | Rows with a matching PK but at least one column value differs |
| `STATUS`     | `OK` (no differences) or `DIFF`                               |

With `--inspect N`, actual row data is shown side by side. For mysql-only and pg-only rows the script also confirms whether the same PK exists on the other side — a `YES (sort-key mismatch)` result means the rows are there but a column-type difference (e.g. `id` stored as `text` in PG instead of `int`) caused the merge sort to not pair them.

Reports are saved to `$LOG_DIR/compare-mysql-pg-data_<timestamp>.txt` (default: `~/migration/logs/`).

### Standalone comparison

`compare-mysql-pg.sh` performs a comprehensive comparison between MySQL and PostgreSQL without needing pg_chameleon. Checks table inventories, row counts, column types (with MySQL→PG type mapping), PK fingerprints (COUNT/MIN/MAX), AUTO_INCREMENT vs sequences, and foreign keys.

```bash
# Full comparison (per-table details including column type mapping)
bash compare-mysql-pg.sh --env-file migration.env

# Quiet mode — only show mismatches and summary
bash compare-mysql-pg.sh --env-file migration.env --quiet

# Fast mode — estimated counts, batch checks, no per-table details
# Best for large databases (hundreds of tables / billions of rows)
bash compare-mysql-pg.sh --env-file migration.env --fast

# Check zero-date values (MySQL '0000-00-00' → PostgreSQL NULL conversion)
bash compare-mysql-pg.sh --env-file migration.env --zero-dates

# Combine: fast scan + zero-date check
bash compare-mysql-pg.sh --env-file migration.env --fast --zero-dates
```

The `--zero-dates` flag scans all `DATE`/`DATETIME`/`TIMESTAMP` columns for MySQL zero-date values and verifies pg_chameleon converted them to `NULL`. Reported per column:

| Status     | Meaning                                                             |
| ---------- | ------------------------------------------------------------------- |
| `OK`       | PG NULLs ≥ MySQL zeros + MySQL NULLs — all zero-dates accounted for |
| `PARTIAL`  | Some zero-date rows may have been discarded during migration        |
| `MISMATCH` | PG NULLs < MySQL NULLs — legitimate NULLs are also missing          |
| `ERR`      | Query failed (table missing in PG or column type mismatch)          |

Reports are saved to `$LOG_DIR/compare-mysql-pg_<timestamp>.txt` (default: `~/migration/logs/`).

### Password cleanup

Password files (`~/.mysql_migration_pw`, `~/.pg_migration_pw`) are **always shredded** after any command completes. To keep them (e.g. during iterative testing):

```bash
python3 chameleon.py start --env-file migration.env --no-shred-passwords
```

---

## Known caveats and gotchas

### Container naming

Replica containers are named `chameleon-replica-{pg_db}-{source}` (e.g. `chameleon-replica-mydb-licenses`). The `pg_db` segment prevents name collisions when two env files replicate from the same MySQL source into different PostgreSQL databases. If old containers named `chameleon-replica-{source}` (without `pg_db`) exist, `start` removes them automatically.

### MySQL database names with hyphens

pg_chameleon embeds the source name in unquoted PostgreSQL identifiers. Hyphens are invalid in unquoted PG identifiers and cause a `SyntaxError`. `chameleon.py` automatically replaces hyphens with underscores in the internal pg_chameleon source name (e.g. `my-db` → `my_db`). The original MySQL database name is still used for MySQL queries and `schema_mappings`.

### GTID replication and empty binlog position

With `CHAMELEON_GTID_ENABLE=true` (the default), once the replica reaches a consistent state (`b_consistent=t`), pg_chameleon clears `t_binlog_name` and `i_binlog_position`. This is normal — position is tracked via the GTID set. `status` prints a note confirming this. `start` validates the binlog position after `init_replica` and refuses to launch the replica container if nothing was captured (indicating a grants or GTID config problem).

### Stale PID file restart loop

Inside a Docker container, the main process always has PID 1. On restart, pg_chameleon finds the old PID file, sees PID 1 is running (itself), concludes another instance is active, and exits — triggering an infinite restart loop. `chameleon.py start` deletes any stale PID file before launching the replica container.

### OOM kill during init_replica (exit 137)

Exit code 137 means the container was OOM-killed by Docker. Increase `CHAMELEON_MEMORY_LIMIT` or reduce `CHAMELEON_COPY_MAX_MEMORY`. Rule of thumb: `MEMORY_LIMIT ≥ 2 × COPY_MAX_MEMORY`. `chameleon.py start` warns at startup if this ratio is violated.

### Tables without primary keys

Tables with no primary or unique key are initialised by `init_replica` but will **not** receive replica updates — pg_chameleon cannot track changes without a key. Run `diagnose` to list affected tables before starting replication.

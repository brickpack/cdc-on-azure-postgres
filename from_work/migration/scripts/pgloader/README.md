# MySQL → PostgreSQL Migration Scripts

Bash scripts for migrating a MySQL database to PostgreSQL using pgloader (via Docker).

## Scripts

Run in order:

| #   | Script                         | Purpose                                                                                                                                                                                                |
| --- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `1-mysql-objects-inventory.sh` | Pre-flight check — lists MySQL objects that pgloader **won't** migrate (views, stored procs, functions, triggers, FULLTEXT indexes, events). These must be recreated manually on PostgreSQL.           |
| 2   | `2-run-pgloader.sh`            | Runs the actual data migration using pgloader in Docker. Auto-detects zero-date defaults, NOT NULL violations, and charset-introducer defaults before migration and emits correct CAST rules for each. |
| 3   | `3-compare-mysql-pg.sh`        | Post-migration validation — compares row counts, column types, PK fingerprints, sequences, indexes, and foreign keys; supports `--fast`, `--quiet`, and `--zero-dates` modes.                          |

### Utility

| Script               | Purpose                                                                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| `write-passwords.sh` | Prompts for MySQL/PG passwords and writes them to `~/.mysql_migration_pw` and `~/.pg_migration_pw` (mode 0600). Keeps secrets out of shell history. |

## VM Setup (fresh deploy)

### Prerequisites (apt)

```bash
sudo apt-get update -qq
sudo apt-get install -y -qq \
  docker-ce docker-ce-cli containerd.io \
  mysql-client postgresql-client-16 \
  python3.11 python3-pip python3.11-venv \
  jq htop
sudo usermod -aG docker "$USER" && newgrp docker
```

> If your VM was provisioned with a bootstrap step, these are usually already present.

### Azure CLI

```bash
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
az login --identity          # uses the VM's managed identity
az account set -s <SUB_ID>   # target subscription
```

### Deploy scripts to the VM

```bash
VM="az ssh vm -g <RG> -n <VM> --prefer-private-ip --"
$VM "mkdir -p ~/migration/scripts/pgloader"
for f in 1-mysql-objects-inventory.sh 2-run-pgloader.sh 3-compare-mysql-pg.sh write-passwords.sh migration.env.example README.md; do
  $VM "cat > ~/migration/scripts/pgloader/$f" < "$f"
done
```

## Configuration

Copy `migration.env.example` to `migration.env` and fill in your values:

```env
MYSQL_FQDN=<mysql db instance>.mysql.database.azure.com
MYSQL_USER=migrate_admin
MYSQL_DB=<source db>
PG_FQDN=<the db instance>.postgres.database.azure.com
PG_USER=<app owner user>
PG_DB=<target db>
PG_SCHEMA=<target schema, e.g. desktop or public>
```

All scripts accept connection details via:

1. **CLI flags** (highest priority): `--mysql-host`, `--mysql-user`, `--mysql-db`, `--pg-host`, `--pg-user`, `--pg-db`, `--pg-schema`
2. **Env file**: `--env-file ./migration.env` (sourced as shell variables)
3. **Environment variables**: `MYSQL_FQDN`, `MYSQL_USER`, `MYSQL_DB`, `PG_FQDN`, `PG_USER`, `PG_DB`, `PG_SCHEMA`

### Passwords

Resolved in order (first found wins):

1. `--mysql-pass-file` / `--pg-pass-file` flags
2. `MYSQL_PASSWORD` / `PG_PASSWORD` environment variables
3. Well-known files: `~/.mysql_migration_pw`, `~/.pg_migration_pw`, `/etc/secrets/mysql_pw`, `/etc/secrets/pg_pw`

Use `write-passwords.sh` to create the well-known files safely.

## Quick Start

```bash
# 1. Write passwords (interactive, no secrets in history)
bash write-passwords.sh

# 2. Check for objects that need manual migration
bash 1-mysql-objects-inventory.sh --env-file ./migration.env

# 3. Dry-run to verify connectivity and list tables
bash 2-run-pgloader.sh --env-file ./migration.env --dry-run

# 4. Run the migration
bash 2-run-pgloader.sh --env-file ./migration.env

# 5. Validate schema and row counts
bash 3-compare-mysql-pg.sh --env-file ./migration.env

# 6. Deep data comparison — checks actual row content, not just counts
python3 ../pg_chameleon/compare-mysql-pg-data.py --env-file ./migration.env --inspect 5
```

## Tuning

All variables are optional. The values shown are the defaults.

| Env var                        | Default            | Notes                                                                                                                     |
| ------------------------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `PGLOADER_WORKERS`             | `4`                | pgloader worker threads; reduce if MySQL CPU spikes                                                                       |
| `PGLOADER_CONCURRENCY`         | `1`                | pgloader concurrency; keep at 1 for Azure MySQL                                                                           |
| `PGLOADER_NET_READ_TIMEOUT`    | `3600`             | MySQL `net_read_timeout` set for each worker connection (seconds)                                                         |
| `PGLOADER_NET_WRITE_TIMEOUT`   | `3600`             | MySQL `net_write_timeout` set for each worker connection (seconds)                                                        |
| `PGLOADER_NULL_SCAN_THRESHOLD` | `500000`           | Tables with estimated rows above this skip the NULL data scan (see [NOT NULL handling](#not-null-handling-pre-migration)) |
| `LOG_DIR`                      | `~/migration/logs` | Directory for log files and per-run error dirs                                                                            |

> The script also attempts `SET GLOBAL net_read_timeout` / `net_write_timeout` at startup. This requires `SUPER` or `SYSTEM_VARIABLES_ADMIN`; failure is non-fatal (a note is printed). For Azure MySQL Flexible Server, set these in the Portal or via CLI as well:
>
> ```bash
> az mysql flexible-server parameter set -g <RG> -s <SERVER> -n net_read_timeout --value 3600
> az mysql flexible-server parameter set -g <RG> -s <SERVER> -n net_write_timeout --value 3600
> ```

## Usage

### Running the migration

```bash
bash 2-run-pgloader.sh --env-file ./migration.env
```

Options:

| Flag                    | Purpose                                                                |
| ----------------------- | ---------------------------------------------------------------------- |
| `--dry-run`             | Verify connectivity and list tables without migrating                  |
| `--exclude-tables LIST` | Comma-separated table names to skip (added to `EXCLUDING TABLE NAMES`) |
| `--schema SCHEMA`       | Target PostgreSQL schema (default: `public`, or `PG_SCHEMA` from env)  |

#### What happens before migration

The script runs three pre-migration scans against MySQL `information_schema` and emits CAST rules into the pgloader `.load` file automatically:

**1. AUTO_INCREMENT with extra qualifiers**
Columns with `AUTO_INCREMENT` plus another `EXTRA` qualifier (e.g. `INVISIBLE`) don't match pgloader's generic `with extra auto_increment` type rule. The script detects these and adds explicit `column <table>.<col> to serial/bigserial` rules.

**2. Charset-introducer defaults**
Columns whose `COLUMN_DEFAULT` starts with `_utf8` (e.g. `_utf8mb3'value'`) produce invalid PostgreSQL DDL. The script detects these and emits `column <table>.<col> to text drop default`.

**3. Zero-date defaults**
MySQL allows date defaults of `0000-00-00` or `0000-00-00 00:00:00`, which PostgreSQL rejects at `CREATE TABLE` time. The script detects all such columns, maps the MySQL type to the correct PostgreSQL type, and emits `column <table>.<col> to <pg_type> drop default drop typemod`.

#### NOT NULL handling (pre-migration) {#not-null-handling-pre-migration}

Legacy MySQL databases sometimes have `NOT NULL` columns that actually contain `NULL` values (enforced when strict mode was disabled). PostgreSQL rejects these during `COPY`, causing pgloader to error or silently drop rows.

The script scans for this **before** migration using a tiered strategy:

- **Small/medium tables** (estimated rows ≤ `PGLOADER_NULL_SCAN_THRESHOLD`, default 500k): runs one `SELECT MAX(col IS NULL), ...` aggregate per table — a single fast scan per table regardless of how many columns it checks.
- **Large tables** (estimated rows > threshold): skips the data scan and emits `drop not null` proactively for **all** `NOT NULL` columns. This is safe (more permissive, never less) and avoids multi-minute full-table scans before migration.

In both cases, if the column already has a CAST rule from one of the scans above, `drop not null` is appended to the existing rule rather than creating a duplicate.

To force scanning all tables regardless of size:

```bash
PGLOADER_NULL_SCAN_THRESHOLD=0 bash 2-run-pgloader.sh --env-file ./migration.env
```

#### Error files

pgloader drops rows it cannot insert (type mismatches, constraint violations, etc.) to error files rather than aborting. The script mounts a per-run directory `$LOG_DIR/errors-<timestamp>/` into the container and reports any `.dat` or `.log` error files at the end of the run. Inspect these to understand any data loss.

### Validation

After migration, `3-compare-mysql-pg.sh` checks:

- Tables present in MySQL but missing in PostgreSQL (and vice versa)
- Foreign key counts
- Row counts (exact or estimated with `--fast`)
- Column names and type mapping (MySQL `COLUMN_TYPE` → PostgreSQL `udt_name`)
- PK fingerprints (`COUNT/MIN/MAX`)
- AUTO_INCREMENT vs sequence presence and sequence values vs `MAX(pk)`

```bash
# Full comparison (per-table details including column type mapping)
bash 3-compare-mysql-pg.sh --env-file ./migration.env

# Quiet mode — only show mismatches and summary
bash 3-compare-mysql-pg.sh --env-file ./migration.env --quiet

# Fast mode — estimated counts via InnoDB statistics; only verified tables get exact COUNT(*)
# Best for large databases (hundreds of tables / billions of rows)
bash 3-compare-mysql-pg.sh --env-file ./migration.env --fast

# Check zero-date values (MySQL '0000-00-00' → PostgreSQL NULL conversion)
bash 3-compare-mysql-pg.sh --env-file ./migration.env --zero-dates

# Combine fast + zero-date check
bash 3-compare-mysql-pg.sh --env-file ./migration.env --fast --zero-dates
```

#### Fast mode

`--fast` uses `information_schema.TABLES.TABLE_ROWS` (MySQL InnoDB estimates) and `pg_class.reltuples` (PostgreSQL estimates) instead of `COUNT(*)`. Tables where estimates differ by more than 0.5% or 50 rows are re-checked with an exact batched `UNION ALL COUNT(*)` query. This makes the comparison finish in seconds on large databases.

#### Zero-date check

`--zero-dates` scans all `DATE`/`DATETIME`/`TIMESTAMP` columns for MySQL zero-date values (`0000-00-00`) and verifies they were converted to `NULL` by pgloader. Reports per column:

| Status     | Meaning                                                             |
| ---------- | ------------------------------------------------------------------- |
| `OK`       | PG NULLs ≥ MySQL zeros + MySQL NULLs — all zero-dates accounted for |
| `PARTIAL`  | Some zero-date rows may have been discarded during migration        |
| `MISMATCH` | PG NULLs < MySQL NULLs — legitimate NULLs are also missing          |
| `ERR`      | Query failed (table missing in PG or column type mismatch)          |

## Logs

All scripts write logs to `~/migration/logs/` (override with `LOG_DIR` env var).

- `pgloader-migrate_<timestamp>.log` — pgloader stdout (summary tables, progress)
- `pgloader-detail-<timestamp>.log` — pgloader verbose log (per-table timings, errors)
- `errors-<timestamp>/` — per-run directory for rows dropped by pgloader (`.dat` + `.log` files)
- `compare-mysql-pg_<timestamp>.txt` — comparison report

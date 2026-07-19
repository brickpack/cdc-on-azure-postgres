# CPCLOUD DB Migration

Tools and runbooks for migrating MySQL workloads to PostgreSQL, with two supported approaches:

- `pgloader` for one-time bulk migrations
- `pg_chameleon` for logical replication and deeper post-migration diagnostics

The repo is organized so you can run either approach independently, depending on the migration phase and risk profile.

## What this repository contains

### 1) pgloader path (simple, one-shot migration)

Location: `migration/scripts/pgloader`

Includes:

- Pre-flight inventory of MySQL objects that do not auto-migrate
- Docker-based pgloader execution
- Post-run MySQL vs PostgreSQL comparison
- Password helper script (`write-passwords.sh`)

Best when:

- You need a straightforward full load
- You can tolerate downtime or a controlled cutover window

### 2) pg_chameleon path (replication-aware migration)

Location: `migration/scripts/pg_chameleon`

Includes:

- Python operator CLI (`chameleon.py`) for start/status/diagnose/validate/logs
- Comparison script and row-diff tooling
- Docker image definition with repo-specific patching
- Environment template and operational runbook

Best when:

- You need ongoing change capture before cutover
- You want richer diagnostics and validation controls

## Quick start

### Prerequisites

Typical runtime requirements (often on a migration VM):

- Docker
- MySQL client
- PostgreSQL client
- Python 3.11+

See tool-specific READMEs for full setup details:

- `migration/scripts/pgloader/README.md`
- `migration/scripts/pg_chameleon/README.md`

### Option A: run with pgloader

```bash
cd migration/scripts/pgloader
cp migration.env.example migration.env
bash write-passwords.sh
bash 1-mysql-objects-inventory.sh --env-file ./migration.env
bash 2-run-pgloader.sh --env-file ./migration.env --dry-run
bash 2-run-pgloader.sh --env-file ./migration.env
bash 3-compare-mysql-pg.sh --env-file ./migration.env
```

### Option B: run with pg_chameleon

```bash
cd migration/scripts/pg_chameleon
cp migration.env.example migration.env
# Password files from pgloader helper are also used if env passwords are unset.
bash ../pgloader/write-passwords.sh
python3 chameleon.py diagnose --env-file migration.env
python3 chameleon.py start --env-file migration.env
python3 chameleon.py status --env-file migration.env
python3 chameleon.py validate --env-file migration.env
```

## Repository layout

```text
.
├── README.md
├── pr-diff.txt
└── migration/
    └── scripts/
        ├── pgloader/
        │   ├── 1-mysql-objects-inventory.sh
        │   ├── 2-run-pgloader.sh
        │   ├── 3-compare-mysql-pg.sh
        │   ├── migration.env.example
        │   ├── write-passwords.sh
        │   └── README.md
        └── pg_chameleon/
            ├── chameleon.py
            ├── compare-mysql-pg.sh
            ├── compare-mysql-pg-data.py
            ├── diff-rows.py
            ├── patch-pg-chameleon.py
            ├── Dockerfile.chameleon
            ├── migration.env.example
            └── README.md
```

## Operational notes

- Keep secrets out of shell history. Prefer password files (`~/.mysql_migration_pw`, `~/.pg_migration_pw`) with mode `0600`.
- Run migration commands from a controlled host (jumpbox/VM) with stable network access to both databases.
- Treat validation as mandatory before cutover: row counts, key consistency, and object parity checks.

## Which path should I choose?

- Start with `pgloader` when you need the fastest path to a full data load.
- Use `pg_chameleon` when you need replication-aware migration and stronger operational observability.
- In complex migrations, teams often use both: initial load with pgloader, then deeper checks and replication-oriented workflows where needed.

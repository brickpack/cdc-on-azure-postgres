# CPCLOUD DB Migration

Toolkit for moving MySQL workloads to Azure PostgreSQL Flexible Server, then
keeping a time-bounded **rollback path** after cutover.

Two folders, three jobs:

| Folder | Job |
| --- | --- |
| [`migration/`](migration/) | Get data onto Postgres (bulk load and/or logical replication) |
| [`cdc/`](cdc/) | After cutover, record every Postgres change so you can replay into MySQL if you must abort |

```text
  MySQL (source)                    Postgres (target)                 MySQL (idle rollback target)
       │                                 ▲                                    ▲
       │  1. migrate                     │                                    │
       ├─────────────────────────────────┤                                    │
       │  pgloader  and/or  pg_chameleon │                                    │
       │                                 │                                    │
       │  2. cut over app writes ────────┘                                    │
       │                                 │                                    │
       │                                 │  3. CDC change log (always on)     │
       │                                 ├──▶ Debezium → Kafka ──┐            │
       │                                 │   (7-day retention)   │ idle until │
       │                                 │                       │ rollback   │
       │                                 │                       └── JDBC ────┘
```

Detailed runbooks live next to each tool. This file is the map.

## Which path when?

| Phase | Tool | Use when |
| --- | --- | --- |
| Initial / one-shot load | **pgloader** | Downtime-tolerant full copy; fastest path to a populated Postgres |
| Online / replication-aware migrate | **pg_chameleon** | Need continuous catch-up from MySQL before cutover, richer diagnose/validate |
| Post-cutover safety net | **CDC** (Compose or AKS) | Keep a 7-day Kafka change log of Postgres so you can roll back to MySQL |

Common pattern: load with pgloader (or chameleon init), run chameleon until
lag is acceptable, cut over, then run CDC for the retention window. Compare
scripts under both migration tools (and CDC’s compare helpers) are for
validation — treat them as mandatory before trusting cutover or rollback.

## Migration tools

### pgloader — one-shot bulk migrate

[`migration/scripts/pgloader/`](migration/scripts/pgloader/)

Dockerized pgloader plus inventory → migrate → compare scripts.

- Pre-flight: objects pgloader will not migrate (views, procs, FULLTEXT, …)
- Auto CAST rules for zero-dates / charset quirks
- Post-run MySQL vs Postgres compare

Start: [`migration/scripts/pgloader/README.md`](migration/scripts/pgloader/README.md)

### pg_chameleon — replication-aware migrate

[`migration/scripts/pg_chameleon/`](migration/scripts/pg_chameleon/)

Python operator (`chameleon.py`) around a patched pg_chameleon image:
start / status / diagnose / validate / fix-sequences / FKs / NOT NULL, plus
row-diff and full data-hash compare scripts.

Start: [`migration/scripts/pg_chameleon/README.md`](migration/scripts/pg_chameleon/README.md)

## CDC rollback pipeline

[`cdc/`](cdc/)

**Not live replication to MySQL.** After cutover, Debezium streams Postgres
into Kafka. The MySQL JDBC sink is deployed only if you trigger rollback.
Retention is 7 days — past that window this path cannot safely restore MySQL.

| Runtime | Where | Docs |
| --- | --- | --- |
| Docker Compose | Single VM, `instances/<name>.env` | [`cdc/README.md`](cdc/README.md) |
| AKS | Strimzi Kafka (RF=3), Key Vault CSI, Helm | [`cdc/aks/README.md`](cdc/aks/README.md) |
| Infra for AKS | RG, VNet, ACR, AKS, CSI role on existing vault | [`cdc/terraform/README.md`](cdc/terraform/README.md) |

Shared for both CDC runtimes (in `cdc/README.md`): architecture, instance
naming (`cdc_<name>` slots/pubs), Postgres/MySQL prep, type-transform checks,
rollback rules, and when **not** to roll back. Install and day-2 commands
stay in the Compose sections of that README or in the AKS runbook.

Order for AKS: terraform apply (+ peering) → aks README install → operate.

## Layout

```text
from_work/
├── README.md                          ← you are here
├── migration/
│   └── scripts/
│       ├── pgloader/                  one-shot MySQL → Postgres
│       └── pg_chameleon/              replication-aware MySQL → Postgres
└── cdc/
    ├── README.md                      product + Docker Compose on a VM
    ├── docker-compose.yml             Compose stack
    ├── connectors/                    Debezium source + MySQL sink templates
    ├── scripts/                       status, monitor, deploy, teardown, load
    ├── instances/                     Compose secrets (*.env, gitignored)
    ├── aks/                           Helm chart + AKS ops scripts / README
    └── terraform/                     Azure infra for the AKS path
```

## Secrets and host

- Prefer password files (`~/.mysql_migration_pw`, `~/.pg_migration_pw`, mode
  `0600`) or Key Vault (AKS CDC) — keep secrets out of shell history and git.
- Run migration and CDC ops from a jump host / worker with stable reachability
  to private Flexible Servers (SSH tunnels as documented in the tool READMEs).

## Next step

1. Migrating a database now → pick **pgloader** or **pg_chameleon** above.
2. Already on Postgres / about to cut over → start with **[`cdc/README.md`](cdc/README.md)**
   (Compose) or **[`cdc/aks/README.md`](cdc/aks/README.md)** after terraform.

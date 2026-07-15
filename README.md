# cdc-on-azure-postgres

Kafka + Debezium pipeline that records every change made to Postgres after a
MySQL -> Postgres migration, so that the migration can be rolled back if
something goes wrong within the retention window.

## Architecture overview

**This is a change log, not live replication.** There is no target database
being kept in sync in real time. Kafka holds an immutable, time-ordered
record of every row change Debezium captured from Postgres. The JDBC sink
that can write that log into MySQL sits idle and unconfigured until a
rollback is actually triggered.

```
                                   ALWAYS RUNNING
                      ┌──────────────────────────────────┐
   Azure Postgres     │   Debezium source connector      │      Kafka topics
  Flexible Server  ───┼─▶ (pgoutput, logical replication)┼──▶  cdc.<schema>.<table>
  (post-cutover       │   continuous, at cutover)        │   (7 days / 168h retention
   system of record)  │                                  │     = your rollback window)
                      └──────────────────────────────────┘                  │
                                                                            │ idle / not deployed
                                                                            │ during normal operation
                                                                            ▼
                                                              ┌──────────────────────────-┐
                                                              │  JDBC sink connector      │
                                                              │  (mysql-sink-standby.json)│
                                                              │  deployed ONLY if rollback│
                                                              │  is triggered             │
                                                              └──────────────┬───────────-┘
                                                                             │ replay
                                                                             ▼
                                                              Azure MySQL Flexible Server
                                                              (original source, kept idle
                                                               post-cutover)
```

Normal operation: Postgres -> Debezium -> Kafka. Nothing downstream of
Kafka is running. MySQL is idle and receives nothing.

Rollback only: the JDBC sink is deployed, pointed at the original MySQL,
and replays the entire Kafka log into it. Once replay catches up, MySQL
contains everything Postgres had at the moment of rollback, and the
application can be flipped back.

## Deployment options

This architecture is deployed one of two ways. Both run the same Kafka
Connect image (root [`Dockerfile`](Dockerfile)) and the same Debezium /
JDBC sink connectors; they differ only in where Kafka + Kafka Connect run
and how secrets/connectors are managed. Pick one -- they are independent,
self-contained setups:

|                         | [`docker/`](docker/)                    | [`aks/`](aks/)                                 |
| ----------------------- | --------------------------------------- | ---------------------------------------------- |
| Where it runs           | Single VM, Docker Compose               | Azure Kubernetes Service (Helm + Strimzi)      |
| Kafka                   | KRaft, single broker                    | Strimzi KRaft, 3 brokers, RF=3                 |
| Secrets                 | `instances/<name>.env` files on the VM  | Azure Key Vault (CSI driver)                   |
| Connectors deployed via | REST API (`docker/scripts/*.sh`)        | `KafkaConnector` CRDs (`helm upgrade`)         |
| Infra provisioning      | Bring your own VM                       | [`aks/terraform/`](aks/terraform/) (optional)  |
| Use when                | Simplest option, one small VM is enough | You need Kafka HA or Key Vault-managed secrets |

Read **[`docker/README.md`](docker/README.md)** or **[`aks/README.md`](aks/README.md)**
for the full setup and operational checklist for whichever you choose. The
rest of this document covers what's shared by both: Postgres prerequisites,
type-transform validation, and the rollback window.

## Prerequisites

Repeat this whole section once per database instance you plan to capture --
each Postgres server needs its own replication user, publication, and
heartbeat table, even though they all feed into the same shared Kafka +
Kafka Connect stack (whichever deployment option you chose).

### Postgres logical replication

On Azure Database for PostgreSQL Flexible Server:

1. Set `wal_level = logical` in the server's parameter blade (or via `az
postgres flexible-server parameter set --name wal_level --value logical`).
   **This requires a server restart on Azure Flexible Server** -- plan for
   a brief connection interruption.
2. Create a dedicated replication user:
   ```sql
   CREATE USER cdc_replication WITH REPLICATION LOGIN PASSWORD 'PLACEHOLDER_REPLICATION_PASSWORD';
   GRANT CONNECT ON DATABASE POSTGRES_DBNAME TO cdc_replication;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_replication;
   ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_replication;
   ```
3. Create the schema and publication covering **all** tables, and set up the
   heartbeat table (run as a Postgres superuser / admin):

   ```sql
   -- Schema that Debezium writes its heartbeat into
   CREATE SCHEMA IF NOT EXISTS cdc;
   GRANT CREATE ON SCHEMA cdc TO cdc_replication;

   -- Capture every table, present and future
   CREATE PUBLICATION PUBLICATION_NAME FOR ALL TABLES;

   -- Heartbeat table -- required for WAL LSN advancement when no other DML
   -- hits a published table (see "Heartbeat" in each deployment's troubleshooting)
   CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (id int PRIMARY KEY, ts timestamptz);
   INSERT INTO cdc.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT DO NOTHING;
   ALTER PUBLICATION PUBLICATION_NAME ADD TABLE cdc.debezium_heartbeat;
   ```

   On AKS, the publication and heartbeat table follow a fixed naming
   convention (`cdc_<name>`) instead of free-form names -- see
   [`aks/README.md`](aks/README.md) prerequisites.

4. Confirm the replication user can create/use logical replication slots
   (this is implicit in the `REPLICATION` role attribute above).

## Type transform validation

Before triggering a real rollback, validate each transform in
`type-transforms.json` (Docker: [`docker/connectors/type-transforms.json`](docker/connectors/type-transforms.json);
AKS: mirrored in [`aks/chart/templates/connectors.yaml`](aks/chart/templates/connectors.yaml))
against a sample row. General pattern: make one test write to Postgres per
data type below, let it flow to Kafka, then (in a non-production test)
deploy the sink against a **throwaway** MySQL database/table first, not the
real original MySQL, and compare.

| Type            | Postgres check                                                                 | MySQL check (after test replay)                                                                                                                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| timestamptz     | `SELECT my_ts_col, my_ts_col AT TIME ZONE 'UTC' FROM my_table WHERE id = 1;`   | `SELECT my_ts_col FROM my_table WHERE id = 1;` -- must equal the UTC value, not local server time                                                                                                                                                           |
| boolean         | `SELECT my_bool_col FROM my_table WHERE id = 1;`                               | `SELECT my_bool_col, my_bool_col + 0 FROM my_table WHERE id = 1;` -- must be `1` or `0`, never the string `'true'`/`'false'`                                                                                                                                |
| jsonb           | `SELECT my_jsonb_col::text FROM my_table WHERE id = 1;`                        | `SELECT my_json_col FROM my_table WHERE id = 1;` -- must parse as valid JSON and match field-for-field                                                                                                                                                      |
| numeric/decimal | `SELECT my_numeric_col, pg_typeof(my_numeric_col) FROM my_table WHERE id = 1;` | `SELECT my_decimal_col FROM my_table WHERE id = 1;` -- every digit must match, no rounding                                                                                                                                                                  |
| text -> varchar | `SELECT MAX(LENGTH(my_text_col)) FROM my_table;`                               | Compare against the MySQL column's declared length: `SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_name='my_table' AND column_name='my_col';` -- if Postgres max length > MySQL limit, widen the MySQL column before rollback |
| enum/set        | `SELECT DISTINCT my_enum_col FROM my_table;`                                   | `SHOW COLUMNS FROM my_table LIKE 'my_enum_col';` -- compare the label sets; if they differ, you need the `enumWorkaround` custom SMT (see `type-transforms.json`)                                                                                           |

If any row doesn't match, fix the relevant transform or destination column
definition and re-test before proceeding with a real rollback.

## When NOT to rollback

Kafka retains 168 hours (7 days) of changes by default in both deployment
options. If the time between cutover and the decision to roll back exceeds
that window, **older changes have already been deleted from the log** and
the change log is incomplete -- replaying it into MySQL would silently
produce data that is missing changes, not a faithful copy of Postgres. Past
168 hours, this pipeline can no longer be used for a safe rollback; you
would need a different recovery strategy (e.g. a fresh Postgres-to-MySQL
migration in reverse, or restoring from backups).

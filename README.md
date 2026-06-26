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
   Azure Postgres     │   Debezium source connector       │      Kafka topics
  Flexible Server  ───┼─▶ (pgoutput, logical replication) ─┼──▶  cdc.<schema>.<table>
  (post-cutover        │   continuous, since before        │   (7 days / 168h retention
   system of record)   │   cutover)                         │    = your rollback window)
                      └──────────────────────────────────┘                │
                                                                            │ idle / not deployed
                                                                            │ during normal operation
                                                                            ▼
                                                              ┌──────────────────────────┐
                                                              │  JDBC sink connector      │
                                                              │  (mysql-sink-standby.json) │
                                                              │  deployed ONLY if rollback │
                                                              │  is triggered              │
                                                              └──────────────┬───────────┘
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

## Prerequisites

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
   ```
3. Create the publication covering the tables you want captured:
   ```sql
   CREATE PUBLICATION PUBLICATION_NAME FOR TABLE table1, table2, table3;
   ```
   (Debezium's `publication.autocreate.mode=filtered` in
   `connectors/postgres-source.json` will also create it automatically from
   `table.include.list` if it doesn't exist yet, as long as the connection
   user has `CREATE` on the database -- creating it explicitly up front is
   recommended so you control exactly which tables are included.)
4. Confirm the replication user can create/use logical replication slots
   (this is implicit in the `REPLICATION` role attribute above).

## Pre-cutover setup

Run this **before** cutover, while pg_chameleon is still the active
replication path from MySQL to Postgres.

1. Copy `.env.example` to `.env` and fill in real values (Postgres
   connection details, table list, slot/publication names, Kafka data
   directory).
2. Bring up the stack:
   ```bash
   docker compose up -d --build
   ```
3. Deploy the Postgres source connector and watch the initial snapshot:
   ```bash
   ./scripts/deploy-postgres-source.sh
   ```
4. **Do not proceed to cutover until the script reports the snapshot is
   complete and the connector is RUNNING.** If it times out, check:
   ```bash
   docker compose logs kafka-connect | grep -i snapshot
   ```

## Cutover procedure

1. Drop the pg_chameleon replication slot on Postgres (pg_chameleon's own
   slot, not Debezium's -- check pg_chameleon's docs/config for its slot
   name).
2. Flip application connection strings from MySQL to Postgres.
3. Confirm Debezium is capturing changes:
   ```bash
   curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq .
   ```
   Make a small test write against Postgres and confirm a record appears
   on its topic:
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic cdc.<schema>.<table> --from-beginning --max-messages 1
   ```
4. **Keep the original MySQL server running and idle.** Do not stop or
   decommission it -- it is your rollback target.

## Normal operation

Run the monitor on a schedule (cron, systemd timer, etc):

```bash
./scripts/monitor-debezium.sh
```

It checks and prints a `STATUS: HEALTHY` / `STATUS: ACTION REQUIRED`
summary covering:

- Debezium source connector + task state.
- Kafka consumer group lag (informational during normal operation -- there
  should be no active consumer groups since the sink isn't deployed).
- Postgres replication slot WAL lag, compared against `WAL_THRESHOLD_MB`
  (in `.env`, no script edits needed to change it).

What to watch for:

- **Connector not RUNNING**: investigate immediately via `docker compose
  logs kafka-connect`. Every minute it's down is a gap in the change log.
- **Growing WAL lag**: means Debezium is falling behind or stopped
  consuming. Postgres keeps WAL on disk for the slot until it's consumed,
  so sustained growth can fill the Postgres server's storage, not just this
  VM's disk.
- **Kafka retains 168 hours (7 days) of changes** (`KAFKA_LOG_RETENTION_HOURS`
  in `docker-compose.yml`). That is your hard rollback window -- see "When
  NOT to rollback" below.

## Type transform validation

Before triggering a real rollback, validate each transform in
`connectors/type-transforms.json` against a sample row. General pattern:
make one test write to Postgres per data type below, let it flow to Kafka,
then (in a non-production test) deploy the sink against a **throwaway**
MySQL database/table first, not the real original MySQL, and compare.

| Type | Postgres check | MySQL check (after test replay) |
|---|---|---|
| timestamptz | `SELECT my_ts_col, my_ts_col AT TIME ZONE 'UTC' FROM my_table WHERE id = 1;` | `SELECT my_ts_col FROM my_table WHERE id = 1;` -- must equal the UTC value, not local server time |
| boolean | `SELECT my_bool_col FROM my_table WHERE id = 1;` | `SELECT my_bool_col, my_bool_col + 0 FROM my_table WHERE id = 1;` -- must be `1` or `0`, never the string `'true'`/`'false'` |
| jsonb | `SELECT my_jsonb_col::text FROM my_table WHERE id = 1;` | `SELECT my_json_col FROM my_table WHERE id = 1;` -- must parse as valid JSON and match field-for-field |
| numeric/decimal | `SELECT my_numeric_col, pg_typeof(my_numeric_col) FROM my_table WHERE id = 1;` | `SELECT my_decimal_col FROM my_table WHERE id = 1;` -- every digit must match, no rounding |
| text -> varchar | `SELECT MAX(LENGTH(my_text_col)) FROM my_table;` | Compare against the MySQL column's declared length: `SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_name='my_table' AND column_name='my_col';` -- if Postgres max length > MySQL limit, widen the MySQL column before rollback |
| enum/set | `SELECT DISTINCT my_enum_col FROM my_table;` | `SHOW COLUMNS FROM my_table LIKE 'my_enum_col';` -- compare the label sets; if they differ, you need the `enumWorkaround` custom SMT (see `connectors/type-transforms.json`) |

If any row doesn't match, fix the relevant transform or destination column
definition and re-test before proceeding with a real rollback.

## Rollback procedure

Run as a checklist, in order:

1. **Stop application writes** to Postgres (maintenance mode, scale down
   write paths, etc -- outside the scope of this repo).
2. Deploy the sink:
   ```bash
   ./scripts/deploy-rollback-sink.sh <mysql_host> <mysql_port> <mysql_user> <mysql_password> <mysql_dbname>
   ```
   This merges `type-transforms.json` into `mysql-sink-standby.json`, fills
   in the connection details you passed, deploys it, and starts monitoring.
3. **Monitor replay progress** -- the script prints connector state and
   consumer lag every 10 seconds.
4. **Wait for Kafka consumer lag to hit zero** before trusting the data in
   MySQL is complete.
5. Run row count verification on both sides for every table:
   ```sql
   -- Postgres
   SELECT count(*) FROM my_table;
   -- MySQL
   SELECT count(*) FROM my_table;
   ```
   Counts won't always match exactly if deletes/inserts raced with the
   stop-writes step -- investigate any large discrepancy, not just any
   discrepancy.
6. Spot-check critical tables for data integrity, e.g.:
   ```sql
   -- Postgres
   SELECT * FROM my_critical_table ORDER BY updated_at DESC LIMIT 20;
   -- MySQL
   SELECT * FROM my_critical_table ORDER BY updated_at DESC LIMIT 20;
   ```
7. Flip application connection strings back to the original MySQL.
8. Verify the application is functioning against MySQL.
9. Run `./scripts/teardown.sh`.

## Post-rollback cleanup

- Drop the Debezium replication slot on Postgres (printed by
  `teardown.sh`, also here for reference):
  ```sql
  SELECT pg_drop_replication_slot('SLOT_NAME');
  ```
- Decommission the Azure VM once you're certain it's no longer needed.
- Decide what to do with Postgres: since rollback means the migration was
  abandoned, Postgres is now stale. Typical options are to keep it
  read-only for a while for forensics, or decommission it once the
  incident is closed out.

## When NOT to rollback

Kafka retains 168 hours (7 days) of changes. If the time between cutover
and the decision to roll back exceeds that window, **older changes have
already been deleted from the log** and the change log is incomplete --
replaying it into MySQL would silently produce data that is missing
changes, not a faithful copy of Postgres. Past 168 hours, this pipeline can
no longer be used for a safe rollback; you would need a different recovery
strategy (e.g. a fresh Postgres-to-MySQL migration in reverse, or restoring
from backups).

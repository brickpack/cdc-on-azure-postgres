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
   -- hits a published table (see "Heartbeat" in Troubleshooting)
   CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (id int PRIMARY KEY, ts timestamptz);
   INSERT INTO cdc.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT DO NOTHING;
   ALTER PUBLICATION PUBLICATION_NAME ADD TABLE cdc.debezium_heartbeat;
   ```

4. Confirm the replication user can create/use logical replication slots
   (this is implicit in the `REPLICATION` role attribute above).

## Pre-cutover setup

Run this **before** cutover, while pg_chameleon is still the active
replication path from MySQL to Postgres.

1. Copy `.env.example` to `.env` and fill in real values (Postgres
   connection details, slot/publication names, Kafka data directory).
2. Bring up the stack:

   ```bash
   docker compose up -d --build
   ```

   **If the VM has no outbound internet access** (cannot reach Docker Hub or
   Confluent Hub at build time), build and export the images on a machine
   that does have access, then transfer them:

   On your build machine (use `--platform linux/amd64` if it is Apple Silicon):

   ```bash
   # Build the Kafka Connect image
   docker buildx build --platform linux/amd64 \
     --output type=docker,dest=cdc-kafka-connect.tar .
   gzip cdc-kafka-connect.tar

   # Materialize the base images as single-arch to avoid Docker Desktop
   # multi-arch manifest issues with docker save
   echo "FROM confluentinc/cp-zookeeper:7.5.0" | \
     docker buildx build --platform linux/amd64 --load \
     -t confluentinc/cp-zookeeper:7.5.0 -
   echo "FROM confluentinc/cp-kafka:7.5.0" | \
     docker buildx build --platform linux/amd64 --load \
     -t confluentinc/cp-kafka:7.5.0 -

   docker save confluentinc/cp-zookeeper:7.5.0 | gzip > cp-zookeeper.tar.gz
   docker save confluentinc/cp-kafka:7.5.0 | gzip > cp-kafka.tar.gz
   ```

   Copy to the VM:

   ```bash
   scp cdc-kafka-connect.tar.gz cp-zookeeper.tar.gz cp-kafka.tar.gz \
     docker-compose.yml .env \
     <user>@<vm-ip>:~/migration/cdc/
   scp connectors/postgres-source.json <user>@<vm-ip>:~/migration/cdc/connectors/
   scp scripts/deploy-postgres-source.sh scripts/monitor-debezium.sh \
     scripts/teardown.sh <user>@<vm-ip>:~/migration/cdc/scripts/
   ```

   On the VM, load the images and start:

   ```bash
   docker load < cdc-kafka-connect.tar.gz
   docker load < cp-zookeeper.tar.gz
   docker load < cp-kafka.tar.gz
   docker-compose up -d
   ```

   Note: locked-down VMs typically have the older `docker-compose` (V1)
   rather than the `docker compose` V2 plugin. Use `docker-compose` (hyphen)
   if `docker compose` gives an "unknown shorthand flag" error.

3. Deploy the Postgres source connector:
   ```bash
   ./scripts/deploy-postgres-source.sh
   ```
   The script creates the `cdc.debezium_heartbeat` table if it is missing,
   then submits the connector config. Debezium starts in **streaming mode
   immediately** (no snapshot -- it captures changes from the point of
   deploy forward).
4. Confirm the connector reaches `RUNNING` state:
   ```bash
   ./scripts/cdc-status.sh
   ```
   Or quickly:
   ```bash
   curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq .
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

### Comprehensive status report

Run this at any time to get a full picture of pipeline health:

```bash
./scripts/cdc-status.sh
```

Sections reported:

- **Connector health** — source and sink connector + task state, with the
  root `Caused by:` line extracted on failure.
- **WAL replication slot** — bytes of unconsumed WAL in Postgres vs
  `WAL_THRESHOLD_MB` (`.env`), plus heartbeat freshness (should be ≤10 s).
- **Consumer group lag** — messages pending per connector.
- **Kafka-Connect log errors** — any `ERROR`/`Exception` lines from the
  last hour.
- **Per-table operation counts** — INSERT / UPDATE / DELETE / TOTAL
  breakdown across every `cdc.*` topic.

To skip the health checks and see only table stats:

```bash
./scripts/cdc-status.sh --tables
```

### Automated health monitor

Run this on a schedule (cron, systemd timer, etc.) -- it exits non-zero if
anything is wrong, making it suitable for alerting:

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

| Type            | Postgres check                                                                 | MySQL check (after test replay)                                                                                                                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| timestamptz     | `SELECT my_ts_col, my_ts_col AT TIME ZONE 'UTC' FROM my_table WHERE id = 1;`   | `SELECT my_ts_col FROM my_table WHERE id = 1;` -- must equal the UTC value, not local server time                                                                                                                                                           |
| boolean         | `SELECT my_bool_col FROM my_table WHERE id = 1;`                               | `SELECT my_bool_col, my_bool_col + 0 FROM my_table WHERE id = 1;` -- must be `1` or `0`, never the string `'true'`/`'false'`                                                                                                                                |
| jsonb           | `SELECT my_jsonb_col::text FROM my_table WHERE id = 1;`                        | `SELECT my_json_col FROM my_table WHERE id = 1;` -- must parse as valid JSON and match field-for-field                                                                                                                                                      |
| numeric/decimal | `SELECT my_numeric_col, pg_typeof(my_numeric_col) FROM my_table WHERE id = 1;` | `SELECT my_decimal_col FROM my_table WHERE id = 1;` -- every digit must match, no rounding                                                                                                                                                                  |
| text -> varchar | `SELECT MAX(LENGTH(my_text_col)) FROM my_table;`                               | Compare against the MySQL column's declared length: `SELECT CHARACTER_MAXIMUM_LENGTH FROM information_schema.columns WHERE table_name='my_table' AND column_name='my_col';` -- if Postgres max length > MySQL limit, widen the MySQL column before rollback |
| enum/set        | `SELECT DISTINCT my_enum_col FROM my_table;`                                   | `SHOW COLUMNS FROM my_table LIKE 'my_enum_col';` -- compare the label sets; if they differ, you need the `enumWorkaround` custom SMT (see `connectors/type-transforms.json`)                                                                                |

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

## Troubleshooting

**`docker compose` not found on the VM**

The VM likely has Docker but not the Compose V2 plugin. Install it offline
(download on a machine with internet access, then copy over):

```bash
# On your build machine
curl -fsSL -o docker-compose-linux-x86_64 \
  "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-x86_64"
scp docker-compose-linux-x86_64 <user>@<vm-ip>:~/

# On the VM
mkdir -p ~/.docker/cli-plugins
mv ~/docker-compose-linux-x86_64 ~/.docker/cli-plugins/docker-compose
chmod +x ~/.docker/cli-plugins/docker-compose
docker compose version  # should print v2.27.0
```

---

**`docker save` fails with "content digest not found" on macOS**

Docker Desktop on Apple Silicon caches multi-arch manifest indices that
`docker save` can't serialise. Use `buildx --load` to force a clean
single-arch image into the local daemon before saving:

```bash
echo "FROM confluentinc/cp-zookeeper:7.5.0" | \
  docker buildx build --platform linux/amd64 --load \
  -t confluentinc/cp-zookeeper:7.5.0 -
docker save confluentinc/cp-zookeeper:7.5.0 | gzip > cp-zookeeper.tar.gz
```

Repeat for any image that fails.

---

**WAL lag frozen / connector stuck at "Searching for WAL resume position"**

Symptom: Debezium connects, transitions to streaming mode, but
`confirmed_flush_lsn` on the replication slot never advances -- even though
the connector shows `RUNNING`. WAL lag grows until Postgres storage fills up.

Root cause: the `pgoutput` plugin only sends WAL records for tables that
are in the publication. If no DML touches those tables, there is nothing
to confirm. Without `heartbeat.action.query`, Debezium has no way to
generate its own publishable record to advance the LSN.

Fix: ensure `heartbeat.action.query` is set (it is in
`connectors/postgres-source.json`) and the heartbeat table exists and is
in the publication -- see Prerequisites step 3. If the slot is already
stuck, manually run the heartbeat update once to break the deadlock:

```sql
UPDATE cdc.debezium_heartbeat SET ts = now() WHERE id = 1;
```

Then watch `confirmed_flush_lsn` advance in `pg_replication_slots`.

---

**JDBC sink task FAILED: "null key schema"**

Symptom: `mysql-rollback-sink-connector` task fails with a trace containing
`NullPointerException` or "null key schema" shortly after deployment.

Root cause: `CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE` was `false` when the
source connector was running. Messages already in the topic have
schema-less JSON keys. The JDBC sink's `pk.mode=record_key` requires a
key schema to map types.

Fix: `docker-compose.yml` already has both key and value schema converters
set to `"true"`. If this error appears it means existing topics contain
schema-less messages. You must reset the topic (all data in it is lost):

```bash
# Delete the affected topic -- Kafka recreates it on next produce
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic <topic-name>

# After deletion, verify the internal Connect topics are still compact
docker compose exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name cdc-connect-offsets --describe | grep cleanup
# Must show cleanup.policy=compact. If it shows delete, fix it:
docker compose exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name cdc-connect-offsets \
  --alter --add-config cleanup.policy=compact
```

Repeat the `cleanup.policy` check and fix for `cdc-connect-configs` and
`cdc-connect-status`.

---

**Debezium DELETEs not appearing in MySQL after rollback**

Symptom: rows that were deleted in Postgres still exist in MySQL after
replay completes.

Root cause: `transforms.unwrap.delete.handling.mode` was `drop` in
`connectors/type-transforms.json`, which silently discarded every DELETE
event before it reached the JDBC sink.

Fix: `type-transforms.json` already has `delete.handling.mode: none`.
If upgrading from an older version, replace `"drop"` with `"none"` in
that file and redeploy the sink.

---

**Image loads without a tag (`Loaded image ID: sha256:...`)**

`docker buildx build --output type=docker` produces an untagged tar.
Re-tag after loading:

```bash
docker load < cdc-kafka-connect.tar.gz
docker tag <sha256-id> cdc-kafka-connect:latest
```

---

**Zookeeper container never becomes healthy**

Check the logs -- if Zookeeper is actually running and listening on 2181
but the healthcheck keeps failing, the four-letter-word command used by the
check (`ruok`) may be blocked. The healthcheck in `docker-compose.yml` uses
`srvr` instead, which is always whitelisted by default. If you see this
after a fresh deploy, confirm you have the latest `docker-compose.yml`.

---

# <!--

AKS DEPLOYMENT
This section is self-contained and can be moved to aks/README.md (or a
separate document) when the Docker path is retired.
================================================================================
-->

## AKS deployment

The `aks/` directory contains a Helm chart that deploys the same pipeline on
Azure Kubernetes Service using Strimzi (KRaft, 3-broker cluster) and Azure
Key Vault CSI for secrets. It supports **multiple database instances** in a
single cluster -- each instance gets its own Debezium source connector and
an isolated Kafka topic prefix (`cdc-<name>`).

### Architecture differences from Docker Compose

|                         | Docker Compose                    | AKS (Helm)                                      |
| ----------------------- | --------------------------------- | ----------------------------------------------- |
| Kafka                   | ZooKeeper + single broker         | Strimzi KRaft, 3 brokers RF=3                   |
| Secrets                 | `.env` file on the VM             | Azure Key Vault CSI                             |
| Connectors deployed via | REST API (`scripts/`)             | `KafkaConnector` CRDs (`helm upgrade`)          |
| Scale                   | 1 database instance               | Up to N instances (5 declared in `values.yaml`) |
| Rollback triggered via  | `scripts/deploy-rollback-sink.sh` | `aks/scripts/deploy-rollback-sink.sh`           |

### Quick start

See [`aks/README.md`](aks/README.md) for the full prerequisite checklist
(Strimzi operator, ACR, Key Vault secrets). The short version:

```bash
# 1. Copy and fill in values
cp aks/values.example.yaml aks/values.local.yaml
# Edit values.local.yaml: set connectImage, keyVault.*, and one instances[]
# entry per database. NO passwords go in this file.

# 2. Install (first time)
helm install cdc-rollback aks/chart -n cdc-rollback \
  -f aks/values.local.yaml

# 3. Upgrade after config changes
helm upgrade cdc-rollback aks/chart -n cdc-rollback \
  -f aks/values.local.yaml
```

### One-time Postgres prerequisites (per instance)

Run as a Postgres superuser on each source database before deploying the
chart. The publication and heartbeat table names follow a fixed convention
based on the instance name (`cdc_<name>`):

```sql
-- Replace <name> with the instance name from values.local.yaml (e.g. billing)
CREATE SCHEMA IF NOT EXISTS cdc;
GRANT CREATE ON SCHEMA cdc TO <postgres.user>;

CREATE PUBLICATION cdc_<name> FOR ALL TABLES;

CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (id int PRIMARY KEY, ts timestamptz);
INSERT INTO cdc.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT DO NOTHING;
ALTER PUBLICATION cdc_<name> ADD TABLE cdc.debezium_heartbeat;
```

### Monitoring (AKS mode)

`scripts/cdc-status.sh` supports AKS via `--mode aks`. The Kafka Connect
REST API must be reachable -- use a port-forward if it is not on a
LoadBalancer:

```bash
kubectl port-forward -n cdc-rollback svc/cdc-connect-connect-api 8083:8083 &

CONNECT_URL=http://localhost:8083 \
POSTGRES_HOST=<host> POSTGRES_PORT=5432 \
POSTGRES_USER=<user> POSTGRES_PASSWORD=<pass> POSTGRES_DBNAME=<db> \
SLOT_NAME=cdc_billing \
  ./scripts/cdc-status.sh --mode aks --instance billing
```

### Triggering a rollback (AKS)

```bash
# Reads MySQL target from the deployed Helm values; password from Key Vault
aks/scripts/deploy-rollback-sink.sh billing
```

### Rollback window

Same 7-day / 168-hour rule as Docker. Configured in `aks/chart/values.yaml`
(`kafka.retentionHours`).

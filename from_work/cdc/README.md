# cdc-on-azure-postgres

Kafka + Debezium pipeline that records every change made to Postgres after a
MySQL → Postgres migration, so the migration can be rolled back if something
goes wrong within the retention window.

**Two runtimes, one pipeline.** This README is the product guide (architecture,
DB prep, rollback rules) plus the **Docker Compose on a VM** path. For AKS
(Helm + Strimzi + Key Vault), use [`aks/README.md`](aks/README.md) after the
shared sections below.

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
  Flexible Server  ───┼─▶ (pgoutput, logical replication)┼──▶  cdc-<name>.<schema>.<table>
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

Normal operation: Postgres → Debezium → Kafka. Nothing downstream of Kafka
is running. MySQL is idle and receives nothing.

Rollback only: the JDBC sink is deployed, pointed at the original MySQL,
and replays the entire Kafka log into it. Once replay catches up, MySQL
contains everything Postgres had at the moment of rollback, and the
application can be flipped back.

## Choose your runtime

| | Docker Compose (this README) | AKS ([`aks/README.md`](aks/README.md)) |
|---|---|---|
| Where it runs | One VM (`docker compose`) | AKS + Strimzi Helm chart |
| Kafka | Single broker, RF=1 | 3 brokers, RF=3 |
| Secrets | `instances/<name>.env` on the VM | Azure Key Vault CSI |
| Deploy connectors | REST scripts (`deploy-*.sh`) | `KafkaConnector` CRs / `helm upgrade` |
| When to use | Lab, single VM, air-gapped image copy | HA rollback log, Key Vault, multi-DB ops |

Shared for both: architecture above, Postgres/MySQL prep, instance naming,
type-transform validation, rollback checklist concepts, and “When NOT to
rollback.” Platform-specific install and day-2 commands stay in their own
sections / the aks README.

## Instance naming (both runtimes)

An **instance** is the pipeline id used for connectors, topics, slots,
publications, and secrets. Prefer a short lowercase nickname (e.g. schema
`toolbox` → instance `toolbox`):

| Artifact | Name |
|---|---|
| Topic prefix | `cdc-<name>` (e.g. `cdc-toolbox.public.orders`) |
| Source / sink connectors | `postgres-source-<name>`, `mysql-rollback-sink-<name>` |
| Publication + slot (default) | `cdc_<name>` (e.g. `cdc_toolbox`) |
| Compose secrets | `instances/<name>.env` |
| AKS config | `instances[].name` in `aks/values.local.yaml` |

Defaults match the Helm chart. Older `<DB_NAME>_cdc_slot` /
`<DB_NAME>_cdc_publication` names still work if you set `SLOT_NAME` /
`PUBLICATION_NAME` (Compose) or `postgres.slotName` /
`postgres.publicationName` (AKS). Prefer creating the publication as
`cdc_<name>` and letting Debezium create the slot on first start.

## Prerequisites

Canonical SQL is in [`scripts/cdc_setup.sql`](scripts/cdc_setup.sql). Summary
below. Split setup by scope:

- **Once per Postgres server**: `wal_level=logical`, shared `cdc_replication`
  role.
- **Once per database** you capture on that server: grants, publication,
  heartbeat table (slot optional — Debezium can create it).

### Postgres logical replication

On Azure Database for PostgreSQL Flexible Server:

1. Set `wal_level = logical` in the server's parameter blade (or via `az
postgres flexible-server parameter set --name wal_level --value logical`).
   **This requires a server restart on Azure Flexible Server** — plan for
   a brief connection interruption.
2. Once per server, create a dedicated replication user:
   ```sql
   CREATE USER cdc_replication WITH REPLICATION LOGIN PASSWORD 'PLACEHOLDER_REPLICATION_PASSWORD';
   ```
3. Once per database, grant access to application schemas (replace
   `APP_SCHEMA` / `POSTGRES_DBNAME` as needed):

   ```sql
   GRANT CONNECT ON DATABASE POSTGRES_DBNAME TO cdc_replication;
   GRANT USAGE ON SCHEMA <APP_SCHEMA> TO cdc_replication;
   GRANT SELECT ON ALL TABLES IN SCHEMA <APP_SCHEMA> TO cdc_replication;
   GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA <APP_SCHEMA> TO cdc_replication;
   ALTER DEFAULT PRIVILEGES IN SCHEMA <APP_SCHEMA>
     GRANT SELECT ON TABLES TO cdc_replication;
   ALTER DEFAULT PRIVILEGES IN SCHEMA <APP_SCHEMA>
     GRANT SELECT, USAGE ON SEQUENCES TO cdc_replication;
   ```

4. Heartbeat schema, publication, and heartbeat table (admin). Use the
   **instance nickname**, not the Azure DB name, unless you override slot /
   publication in config:

   ```sql
   CREATE SCHEMA IF NOT EXISTS cdc;
   GRANT USAGE, CREATE ON SCHEMA cdc TO cdc_replication;

   -- App + heartbeat only — not FOR ALL TABLES (excludes sch_chameleon)
   CREATE PUBLICATION cdc_<name> FOR TABLES IN SCHEMA public, cdc;
   -- e.g. CREATE PUBLICATION cdc_toolbox FOR TABLES IN SCHEMA public, cdc;

   CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (
     id int PRIMARY KEY,
     ts timestamptz
   );
   INSERT INTO cdc.debezium_heartbeat (id, ts)
   VALUES (1, now())
   ON CONFLICT DO NOTHING;
   GRANT SELECT, INSERT, UPDATE ON TABLE cdc.debezium_heartbeat TO cdc_replication;
   ```

   Leave the publication owned by the admin role on Azure (schema-level pubs
   require a superuser owner). Do **not** pre-create a slot under a different
   name than the connector’s `slot.name` — orphans pin WAL. Debezium creates
   `cdc_<name>` on first start if missing.

5. Confirm `rolreplication` is true for `cdc_replication`:

   ```sql
   SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'cdc_replication';
   ```

### MySQL rollback target

On the original source Azure Database for MySQL Flexible Server:

1. Once per MySQL server, create a dedicated rollback user (reuse across DBs
   on that server if you want):

   ```sql
   CREATE USER 'cdc_rollback'@'%'
   IDENTIFIED BY 'PLACEHOLDER_MYSQL_PASSWORD';
   ```

2. Once per rollback-target database (`auto.create` / `auto.evolve` are off —
   tables must already exist):

   ```sql
   GRANT SELECT, INSERT, UPDATE, DELETE ON `MYSQL_DBNAME`.* TO 'cdc_rollback'@'%';
   FLUSH PRIVILEGES;
   SHOW GRANTS FOR 'cdc_rollback'@'%';
   ```

### MySQL binlog requirements

```sql
SHOW VARIABLES LIKE 'binlog_format';      -- expect ROW
SHOW VARIABLES LIKE 'binlog_row_image';   -- expect FULL
```

Compose: set each instance’s `ORIGINAL_MYSQL_USER` / password to this
rollback user. AKS: `mysql.user` in `values.local.yaml` + Key Vault secret.

---

## Docker Compose (VM)

Install and day-2 ops for the **single-VM Compose** path (through Normal
operation). Type-transform checks, rollback rules, and “When NOT to
rollback” after that are shared with AKS. Troubleshooting at the end is
Compose-specific. For AKS install, use [`aks/README.md`](aks/README.md)
after Prerequisites above.

### Pre-cutover setup

Run this **before** cutover, while pg_chameleon is still the active
replication path from MySQL to Postgres.

One shared Kafka + Connect stack serves many instances (different Postgres /
MySQL servers). Each instance gets its own connector, topics, slot, and
rollback target — see [Instance naming](#instance-naming-both-runtimes) and
[Adding another instance](#adding-another-instance).

1. For **each** database you want to capture, copy
   `instances/example.env.example` to `instances/<name>.env` (for example,
   `instances/toolbox.env`) and fill in Postgres connection details and
   MySQL rollback credentials. Prefer defaults
   `SLOT_NAME=cdc_<name>` / `PUBLICATION_NAME=cdc_<name>` (matching the
   publication you created in Prerequisites).

2. Copy `.env.example` to `.env` and fill in shared stack settings (Kafka
   data directory, Connect host name, WAL threshold, etc). Do **not** put
   per-database details in `.env`.

3. Build/export images and generate `KAFKA_CLUSTER_ID` on a machine that has
   Docker available (typically your build machine). If your VM has no outbound
   internet access, this build-machine path is required.

   On your build machine (use `--platform linux/amd64` if it is Apple Silicon):

   ```bash
   # Build the Kafka Connect image
   docker buildx build --platform linux/amd64 \
     --output type=docker,dest=cdc-kafka-connect.tar .
   gzip cdc-kafka-connect.tar

   # Prepare the Kafka base image locally (single-arch)
   echo "FROM confluentinc/cp-kafka:7.5.0" | \
     docker buildx build --platform linux/amd64 --load \
     -t confluentinc/cp-kafka:7.5.0 -

   docker save confluentinc/cp-kafka:7.5.0 | gzip > cp-kafka.tar.gz
   ```

4. Copy artifacts and config to the VM, then load images and start containers.

   ```bash
   ssh <user>@<vm-ip> mkdir -p ~/migration/cdc/instances
   scp cdc-kafka-connect.tar.gz cp-kafka.tar.gz \
     docker-compose.yml .env \
     <user>@<vm-ip>:~/migration/cdc/
   scp instances/*.env <user>@<vm-ip>:~/migration/cdc/instances/
   scp connectors/postgres-source.json connectors/mysql-sink-standby.json \
     connectors/type-transforms.json <user>@<vm-ip>:~/migration/cdc/connectors/
   scp scripts/deploy-postgres-source.sh scripts/deploy-rollback-sink.sh \
     scripts/monitor-debezium.sh scripts/cdc-status.sh \
     scripts/teardown.sh <user>@<vm-ip>:~/migration/cdc/scripts/
   ```

   On the VM:

   ```bash
   docker load < cdc-kafka-connect.tar.gz
   docker tag $(docker images -q | head -1) cdc-kafka-connect:latest
   docker load < cp-kafka.tar.gz
   ```

5. Generate KRaft cluster ID on the VM and write it into .env

   ```bash
   KAFKA_CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:7.5.0 kafka-storage random-uuid)
     sed -i.bak "s|^KAFKA_CLUSTER_ID=.*|KAFKA_CLUSTER_ID=${KAFKA_CLUSTER_ID}|" .env && rm .env.bak

   # Start Docker Cluster
   docker compose up -d
   ```

6. Deploy the Postgres source connector(s):

```bash
./scripts/deploy-postgres-source.sh              # every instance under instances/
./scripts/deploy-postgres-source.sh toolbox       # just one instance (e.g. adding one later)
```

For each instance, the script creates the `cdc.debezium_heartbeat` table
if it is missing, then submits the connector config. Debezium starts in
**streaming mode immediately** (no snapshot -- it captures changes from
the point of deploy forward).

7. Confirm every connector reaches `RUNNING` state:

```bash
./scripts/cdc-status.sh                       # if only one instance is configured
./scripts/cdc-status.sh --instance toolbox    # required once more than one instance exists
```

Or quickly (substitute your instance name):

```bash
curl -s http://localhost:8083/connectors/postgres-source-toolbox/status | jq .
```

### Cutover procedure

Repeat this checklist independently for each instance -- cutting one
database over does not require pausing or touching any other instance's
pipeline.

1. Drop the pg_chameleon replication slot on that instance's Postgres
   (pg_chameleon's own slot, not Debezium's -- check pg_chameleon's
   docs/config for its slot name).
2. Flip that application's connection strings from MySQL to Postgres.
3. Confirm Debezium is capturing changes (substitute the instance name,
   e.g. `toolbox`):
   ```bash
   curl -s http://localhost:8083/connectors/postgres-source-toolbox/status | jq .
   ```
   Make a small test write against Postgres and confirm a record appears
   on its topic (topics are named `cdc-<name>.<schema>.<table>`):
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic cdc-toolbox.<schema>.<table> --from-beginning --max-messages 1
   ```
4. **Keep that instance's original MySQL server running and idle.** Do not
   stop or decommission it -- it is that database's rollback target.

### Adding another instance

Onboarding another database (a different Postgres server, a different
original MySQL server) does not require a second Kafka/Kafka Connect stack
and does not disrupt any instance already running:

1. Run the instance's Postgres prerequisites (above) -- replication user,
   `cdc` schema, publication, heartbeat table -- against its own server.
2. Copy `instances/example.env.example` to `instances/<name>.env` and fill
   in that instance's details (Postgres source + original MySQL rollback
   target). Prefer `SLOT_NAME=cdc_<name>` / `PUBLICATION_NAME=cdc_<name>`.
3. Deploy just that instance's source connector:
   ```bash
   ./scripts/deploy-postgres-source.sh <name>
   ```
   The Kafka Connect worker picks it up alongside every other instance's
   connector; nothing else needs restarting.
4. Confirm it reaches `RUNNING`:
   ```bash
   ./scripts/cdc-status.sh --instance <name>
   ```

To remove an instance entirely once it's no longer needed (not a rollback --
just decommissioning), delete its connectors and drop its replication slot:
see `./scripts/teardown.sh <name>` in "Post-rollback cleanup" below.

### Normal operation

#### Comprehensive status report

Run this at any time to get a full picture of pipeline health. With only
one instance configured under `instances/`, no flag is needed; with more
than one, pass `--instance <name>`:

```bash
./scripts/cdc-status.sh
./scripts/cdc-status.sh --instance toolbox
```

Sections reported:

- **Connector health** — source and sink connector + task state, with the
  root `Caused by:` line extracted on failure.
- **WAL replication slot** — bytes of unconsumed WAL in Postgres vs
  `WAL_THRESHOLD_MB` (`.env`), plus heartbeat freshness (should be ≤10 s).
- **Consumer group lag** — messages pending per connector.
- **Kafka-Connect log errors** — any `ERROR`/`Exception` lines from the
  last hour.
- **Per-table message counts** — total messages per `cdc-<name>.*` topic.

To skip the health checks and show only table counts (fast):

```bash
./scripts/cdc-status.sh --instance toolbox --tables
```

To also show INSERT / UPDATE / DELETE breakdown (reads every message — slow on large topics):

```bash
./scripts/cdc-status.sh --instance toolbox --tables --ops
```

#### Automated health monitor

Run this on a schedule (cron, systemd timer, etc.) -- it exits non-zero if
anything is wrong, making it suitable for alerting. With no arguments it
checks **every** instance under `instances/`; pass `--instance <name>` to
check just one (useful for per-instance alerting):

```bash
./scripts/monitor-debezium.sh
./scripts/monitor-debezium.sh --instance toolbox
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

---

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

Shared checklist for **one instance**. Other instances keep capturing.
Deploy/monitor commands differ by runtime.

1. **Stop application writes** to that instance's Postgres (maintenance
   mode, scale down write paths, etc -- outside the scope of this repo).
2. **Deploy the MySQL sink** for that instance only:

   **Compose:**
   ```bash
   ./scripts/deploy-rollback-sink.sh <instance_name>
   ```
   Uses `instances/<instance_name>.env` and scopes `topics.regex` to that
   instance.

   **AKS:** see [`aks/README.md` — Rollback](aks/README.md#rollback-procedure-per-instance)
   (`./aks/scripts/deploy-rollback-sink.sh <name>` or
   `helm upgrade … --set-json 'rollback=["<name>"]'`).

3. **Monitor replay** until consumer lag hits zero (Compose script prints
   lag every 10s; AKS: Connect status + consumer group lag as in aks README).
4. Row-count and spot-check critical tables on Postgres vs MySQL.
5. Flip application connection strings back to the original MySQL.
6. Verify the application against MySQL.
7. **Tear down that instance’s connectors** (leave Kafka/Connect up for
   other instances):

   **Compose:** `./scripts/teardown.sh <instance_name>`

   **AKS:** clear rollback list with `--set-json 'rollback=[]'`, then
   decommission source/slot per aks README.

### Post-rollback cleanup

- Drop the Debezium replication slot on that instance's Postgres (Compose
  `teardown.sh` prints the name; default is `cdc_<name>`):
  ```sql
  SELECT pg_drop_replication_slot('cdc_<name>');
  ```
- **Compose:** decommission the Azure VM only once **every** instance is
  done (`./scripts/teardown.sh --all`).
- **AKS:** tear down per [`aks/README.md`](aks/README.md) (Helm/Strimzi);
  do not uninstall the shared stack for one instance.
- Postgres is now stale after rollback — keep read-only for forensics or
  decommission when the incident is closed.

## When NOT to rollback

Kafka retains 168 hours (7 days) of changes (Compose:
`KAFKA_LOG_RETENTION_HOURS`; AKS: chart Kafka config). If the time between
cutover and the decision to roll back exceeds that window, **older changes
have already been deleted from the log** and the change log is incomplete --
replaying it into MySQL would silently produce data that is missing
changes, not a faithful copy of Postgres. Past 168 hours, this pipeline can
no longer be used for a safe rollback; you would need a different recovery
strategy (e.g. a fresh Postgres-to-MySQL migration in reverse, or restoring
from backups).

## Troubleshooting (Docker Compose)

Compose-specific. For AKS pods / connectors, see
[`aks/README.md`](aks/README.md).

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
echo "FROM confluentinc/cp-kafka:7.5.0" | \
  docker buildx build --platform linux/amd64 --load \
  -t confluentinc/cp-kafka:7.5.0 -
docker save confluentinc/cp-kafka:7.5.0 | gzip > cp-kafka.tar.gz
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

## AKS

Same pipeline on AKS (Strimzi Kafka RF=3, Key Vault CSI). Install, monitor,
rollback, and teardown: **[`aks/README.md`](aks/README.md)**. Shared
sections above (architecture, naming, Postgres/MySQL prep, type transforms,
rollback rules, retention window) still apply.

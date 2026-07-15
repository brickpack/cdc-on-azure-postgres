# Docker Compose deployment

Runs the CDC rollback pipeline on a single VM via Docker Compose, for any
number of database instances behind one shared Kafka + Kafka Connect
stack. The AKS/Helm path lives in [`../aks/`](../aks/) and is unaffected by
anything here.

Read the main [README](../README.md) first -- the architecture, Postgres
prerequisites, type-transform validation, and rollback window all apply
unchanged. This file covers the Docker Compose-specific setup and
operational checklist.

## Pre-cutover setup

Run this **before** cutover, while pg_chameleon is still the active
replication path from MySQL to Postgres.

This stack supports multiple database instances (different Postgres
servers, different original MySQL servers) behind **one** shared Kafka +
Kafka Connect deployment -- you do not stand up a separate docker compose
stack per database. Each instance gets its own connector, topic prefix
(`cdc-<name>`), replication slot, and rollback target; see
["Adding another instance"](#adding-another-instance) below.

1. Copy `.env.example` to `.env` and fill in the settings shared by the
   whole stack (Kafka data directory, cluster ID, etc -- no per-database
   details go here).

   Then, for **each** database you want to capture, copy
   `instances/example.env.example` to `instances/<name>.env` (the filename,
   e.g. `instances/billing.env`, becomes the instance name used throughout
   the pipeline) and fill in that instance's Postgres connection details,
   slot name, and publication name.

   **If the VM has no outbound internet access** (cannot reach Docker Hub or
   Confluent Hub at build time), build and export the images on a machine
   that does have access, then transfer them:

   On your build machine (use `--platform linux/amd64` if it is Apple Silicon).
   Run this from the **repo root** (one level up from `docker/`), since
   that's where the `Dockerfile` lives:

   ```bash
   # Build the Kafka Connect image
   docker buildx build --platform linux/amd64 \
     --output type=docker,dest=cdc-kafka-connect.tar .
   gzip cdc-kafka-connect.tar

   # Save the Kafka base image (single-arch to avoid multi-arch manifest issues)
   echo "FROM confluentinc/cp-kafka:7.5.0" | \
     docker buildx build --platform linux/amd64 --load \
     -t confluentinc/cp-kafka:7.5.0 -

   docker save confluentinc/cp-kafka:7.5.0 | gzip > cp-kafka.tar.gz
   ```

   Copy to the VM (run from `docker/`, since that's where
   `docker-compose.yml`, `.env`, `instances/`, `connectors/`, and `scripts/`
   live -- `cdc-kafka-connect.tar.gz` and `cp-kafka.tar.gz` are the ones you
   just built above one directory up):

   ```bash
   ssh <user>@<vm-ip> mkdir -p ~/migration/cdc/instances
   scp ../cdc-kafka-connect.tar.gz ../cp-kafka.tar.gz \
     docker-compose.yml .env \
     <user>@<vm-ip>:~/migration/cdc/
   scp instances/*.env <user>@<vm-ip>:~/migration/cdc/instances/
   scp connectors/postgres-source.json connectors/mysql-sink-standby.json \
     connectors/type-transforms.json <user>@<vm-ip>:~/migration/cdc/connectors/
   scp scripts/deploy-postgres-source.sh scripts/deploy-rollback-sink.sh \
     scripts/monitor-debezium.sh scripts/cdc-status.sh \
     scripts/teardown.sh <user>@<vm-ip>:~/migration/cdc/scripts/
   ```

   On the VM, load the images and start:

   ```bash
   docker load < cdc-kafka-connect.tar.gz
   docker tag $(docker images -q | head -1) cdc-kafka-connect:latest
   docker load < cp-kafka.tar.gz
   docker compose up -d
   ```

2. Deploy the Postgres source connector(s):
   ```bash
   ./scripts/deploy-postgres-source.sh              # every instance under instances/
   ./scripts/deploy-postgres-source.sh billing       # just one instance (e.g. adding one later)
   ```
   For each instance, the script creates the `cdc.debezium_heartbeat` table
   if it is missing, then submits the connector config. Debezium starts in
   **streaming mode immediately** (no snapshot -- it captures changes from
   the point of deploy forward).
3. Confirm every connector reaches `RUNNING` state:
   ```bash
   ./scripts/cdc-status.sh                       # if only one instance is configured
   ./scripts/cdc-status.sh --instance billing    # required once more than one instance exists
   ```
   Or quickly (substitute your instance name):
   ```bash
   curl -s http://localhost:8083/connectors/postgres-source-billing/status | jq .
   ```

## Cutover procedure

Repeat this checklist independently for each instance -- cutting one
database over does not require pausing or touching any other instance's
pipeline.

1. Drop the pg_chameleon replication slot on that instance's Postgres
   (pg_chameleon's own slot, not Debezium's -- check pg_chameleon's
   docs/config for its slot name).
2. Flip that application's connection strings from MySQL to Postgres.
3. Confirm Debezium is capturing changes (substitute the instance name,
   e.g. `billing`):
   ```bash
   curl -s http://localhost:8083/connectors/postgres-source-billing/status | jq .
   ```
   Make a small test write against Postgres and confirm a record appears
   on its topic (topics are named `cdc-<name>.<schema>.<table>`):
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic cdc-billing.<schema>.<table> --from-beginning --max-messages 1
   ```
4. **Keep that instance's original MySQL server running and idle.** Do not
   stop or decommission it -- it is that database's rollback target.

## Adding another instance

Onboarding another database (a different Postgres server, a different
original MySQL server) does not require a second Kafka/Kafka Connect stack
and does not disrupt any instance already running:

1. Run the instance's Postgres prerequisites (main README) -- replication
   user, `cdc` schema, publication, heartbeat table -- against its own
   server.
2. Copy `instances/example.env.example` to `instances/<name>.env` and fill
   in that instance's details.
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

## Normal operation

### Comprehensive status report

Run this at any time to get a full picture of pipeline health. With only
one instance configured under `instances/`, no flag is needed; with more
than one, pass `--instance <name>`:

```bash
./scripts/cdc-status.sh
./scripts/cdc-status.sh --instance billing
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
./scripts/cdc-status.sh --instance billing --tables
```

To also show INSERT / UPDATE / DELETE breakdown (reads every message — slow on large topics):

```bash
./scripts/cdc-status.sh --instance billing --tables --ops
```

### Automated health monitor

Run this on a schedule (cron, systemd timer, etc.) -- it exits non-zero if
anything is wrong, making it suitable for alerting. With no arguments it
checks **every** instance under `instances/`; pass `--instance <name>` to
check just one (useful for per-instance alerting):

```bash
./scripts/monitor-debezium.sh
./scripts/monitor-debezium.sh --instance billing
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
  in `docker-compose.yml`). That is your hard rollback window -- see the
  main README's "When NOT to rollback".

## Rollback procedure

Run as a checklist, in order, **for the one instance being rolled back**.
Other instances are unaffected -- their source connectors keep capturing
changes the entire time.

1. **Stop application writes** to that instance's Postgres (maintenance
   mode, scale down write paths, etc -- outside the scope of this repo).
2. Deploy the sink:
   ```bash
   ./scripts/deploy-rollback-sink.sh <instance_name> <mysql_host> <mysql_port> <mysql_user> <mysql_password> <mysql_dbname>
   ```
   `instance_name` must match `instances/<instance_name>.env`. The script
   merges `type-transforms.json` into `mysql-sink-standby.json`, fills in
   the connection details you passed, scopes `topics.regex` to just this
   instance's topics, deploys it, and starts monitoring.
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
9. Run `./scripts/teardown.sh <instance_name>` to remove just this
   instance's connectors -- Kafka and Kafka Connect keep running for any
   other instances still being captured.

## Post-rollback cleanup

- Drop the Debezium replication slot on that instance's Postgres (printed
  by `teardown.sh`, also here for reference):
  ```sql
  SELECT pg_drop_replication_slot('SLOT_NAME');
  ```
- Decommission the Azure VM once **every** instance it hosts is no longer
  needed -- run `./scripts/teardown.sh --all` only once nothing on the VM
  is still required (this stops Kafka + Connect for every instance).
- Decide what to do with that instance's Postgres: since rollback means the
  migration was abandoned, Postgres is now stale. Typical options are to
  keep it read-only for a while for forensics, or decommission it once the
  incident is closed out.

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
in the publication -- see the main README's Prerequisites step 3. If the
slot is already stuck, manually run the heartbeat update once to break the
deadlock:

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

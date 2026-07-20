
-- PostgreSQL Server Configuration

-- Step 1. Check current replication settings
SHOW wal_level;
SHOW max_replication_slots;
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication_tables;
SHOW max_wal_senders;

REASSIGN OWNED BY cdc_replication TO pgadmin;
DROP OWNED BY cdc_replication;
DROP ROLE cdc_replication;

-- Step 2. Once per server, create the shared CDC replication user
CREATE USER cdc_replication WITH REPLICATION LOGIN PASSWORD 'PLACEHOLDER_REPLICATION_PASSWORD';

-- Step 3. Connect to one captured database and grant access to its real
-- application schema(s). Replace APP_SCHEMA with each schema that contains
-- tables Debezium should capture for this database.
GRANT CONNECT ON DATABASE <DB_NAME> TO cdc_replication;

GRANT USAGE ON SCHEMA <APP_SCHEMA> TO cdc_replication;

GRANT SELECT ON ALL TABLES IN SCHEMA <APP_SCHEMA> TO cdc_replication;

GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA <APP_SCHEMA> TO cdc_replication;

ALTER DEFAULT PRIVILEGES IN SCHEMA <APP_SCHEMA> 
  GRANT SELECT ON TABLES TO cdc_replication;

ALTER DEFAULT PRIVILEGES IN SCHEMA <APP_SCHEMA> 
  GRANT SELECT, USAGE ON SEQUENCES TO cdc_replication;

-- If different owner roles create tables/sequences, run the ALTER DEFAULT
-- PRIVILEGES statements as each owner or use FOR ROLE <owner>.

-- Step 4. In that same database, create the schema that Debezium writes its
-- heartbeat into
CREATE SCHEMA IF NOT EXISTS cdc;

GRANT USAGE, CREATE ON SCHEMA cdc TO cdc_replication;

-- Step 5. Publish only app data + the CDC heartbeat schema.
-- Do NOT use FOR ALL TABLES: that includes sch_chameleon (pg_chameleon
-- replica metadata), which must stay out of the Debezium publication.
-- FOR TABLES IN SCHEMA requires PostgreSQL 15+. On PG 14, list tables
-- explicitly (public.* + cdc.debezium_heartbeat).
--
-- NAME MUST MATCH THE CONNECTOR. AKS Helm defaults both publication and
-- slot to cdc_<instances[].name> (e.g. instance toolbox → cdc_toolbox).
-- The older <DB_NAME>_cdc_publication / <DB_NAME>_cdc_slot pattern only
-- works if you also set postgres.publicationName / postgres.slotName in
-- aks/values.local.yaml (or SLOT_NAME / PUBLICATION_NAME in Docker .env).
-- Example for AKS default (instance name toolbox):
--   CREATE PUBLICATION cdc_toolbox FOR TABLES IN SCHEMA public, cdc;
CREATE PUBLICATION cdc_<DB_NAME> FOR TABLES IN SCHEMA public, cdc;

-- Step 6. Replication slot — OPTIONAL to create by hand.
-- Debezium creates the slot named in slot.name on first start if missing.
-- If you pre-create it, use the SAME name the connector will use
-- (AKS default: cdc_<instances[].name>), or you get an orphan slot that
-- retains WAL until dropped (that is why shop_cdc_slot had to go when the
-- connector came up as cdc_toolbox).
-- SELECT pg_create_logical_replication_slot('cdc_toolbox', 'pgoutput');
SELECT * FROM pg_create_logical_replication_slot('cdc_<DB_NAME>', 'pgoutput');

-- Step 7. Heartbeat table -- required for WAL LSN advancement when no other
-- DML hits a published table (see "Heartbeat" in Troubleshooting)
CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (
id int PRIMARY KEY, 
ts timestamptz
);
INSERT INTO cdc.debezium_heartbeat (id, ts) 
VALUES (1, now()) 
ON CONFLICT DO NOTHING;

GRANT SELECT, INSERT, UPDATE ON TABLE cdc.debezium_heartbeat TO cdc_replication;

-- Verify the role has replication privilege
SELECT rolname, rolreplication
FROM pg_roles
WHERE rolname = 'cdc_replication';



-- MySQL 

-- Step 1. Create the dedicated rollback user for MySQL
CREATE USER 'cdc_rollback'@'%' 
IDENTIFIED BY 'PLACEHOLDER_MYSQL_PASSWORD';

-- Step 2. Grant the necessary privileges to the rollback user for each database
GRANT SELECT, INSERT, UPDATE, DELETE ON `MYSQL_DBNAME`.* TO 'cdc_rollback'@'%';

-- Apply the changes to ensure the new user and privileges take effect
FLUSH PRIVILEGES;

-- Verify the grants:
SHOW GRANTS FOR 'cdc_rollback'@'%';

-- MySQL requirements

-- 1. Check `binlog_format`:
SHOW VARIABLES LIKE 'binlog_format';
-- Expected: `ROW`

-- 2. Check `binlog_row_image`:
SHOW VARIABLES LIKE 'binlog_row_image';
-- Expected: `FULL`

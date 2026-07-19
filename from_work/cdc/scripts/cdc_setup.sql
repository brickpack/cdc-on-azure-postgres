
-- PostgreSQL Server Configuration

-- Step 1. Check current replication settings
SHOW wal_level;
SHOW max_replication_slots;
SELECT * FROM pg_replication_slots;
SELECT * FROM pg_publication_tables;
SHOW max_wal_senders;

REASSIGN OWNED BY cdc_replication TO psqladmin;
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

-- Step 5. Capture every table, present and future
CREATE PUBLICATION <DB_NAME>_cdc_publication FOR ALL TABLES;

-- Step 6. Create the logical replication slot Debezium will use for this
-- database. Set SLOT_NAME in instances/<name>.env to this same value.
SELECT * FROM pg_create_logical_replication_slot('<DB_NAME>_cdc_slot', 'pgoutput');

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




-- Check slot state first
SELECT slot_name, active, active_pid
FROM pg_replication_slots
WHERE slot_name = 'debezium';

-- If active, stop connector first, then terminate backend if still attached
SELECT pg_terminate_backend(active_pid)
FROM pg_replication_slots
WHERE slot_name = 'debezium'
  AND active_pid IS NOT NULL;

-- Drop slot
SELECT pg_drop_replication_slot('debezium');

SELECT pg_drop_replication_slot('debezium');
SELECT current_user;
SELECT rolname, rolreplication
FROM pg_roles
WHERE rolname = current_user;
ALTER ROLE psqladmin WITH REPLICATION;

ALTER PUBLICATION <DB_NAME>_cdc_publication OWNER TO cdc_replication;

DROP PUBLICATION <DB_NAME>_cdc_publication;
CREATE PUBLICATION <DB_NAME>_cdc_publication FOR ALL TABLES;

-- Total rows processed (all batches, all sources)
SELECT SUM(i_replayed) AS total_rows_replayed,
       SUM(i_skipped) AS total_rows_skipped,
       SUM(i_ddl) AS total_ddl_operations
FROM sch_chameleon.t_replica_batch;


-- Pending batches (not yet replayed)
SELECT COUNT(*) AS pending_batches,
       SUM(i_replayed + i_skipped) AS pending_rows
FROM sch_chameleon.t_replica_batch
WHERE NOT b_replayed;


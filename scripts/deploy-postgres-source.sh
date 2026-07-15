#!/usr/bin/env bash
# Deploys the Debezium Postgres source connector (connectors/postgres-source.json)
# for one or more instances. Run this BEFORE cutover -- the initial snapshot
# must finish while pg_chameleon is still the system of record. See README
# "Pre-cutover setup".
#
# Usage:
#   ./deploy-postgres-source.sh              # deploy every instance under instances/
#   ./deploy-postgres-source.sh <name>       # deploy just one instance (instances/<name>.env)
#
# Prerequisite (one-time, run as a Postgres admin before first deploy, per instance):
#   GRANT CREATE ON SCHEMA cdc TO <POSTGRES_USER>;
#   ALTER PUBLICATION <PUBLICATION_NAME> ADD TABLE cdc.debezium_heartbeat;
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"
INSTANCES_DIR="${ROOT_DIR}/instances"
CONNECTOR_FILE="${ROOT_DIR}/connectors/postgres-source.json"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi
: "${CONNECT_URL:=http://localhost:8083}"

deploy_one() {
  local instance_env="$1"
  (
    set -a
    # shellcheck disable=SC1090
    source "$instance_env"
    set +a

    : "${INSTANCE_NAME:?INSTANCE_NAME is not set in ${instance_env}}"
    : "${POSTGRES_HOST:?POSTGRES_HOST is not set in ${instance_env}}"
    : "${POSTGRES_PORT:?POSTGRES_PORT is not set in ${instance_env}}"
    : "${POSTGRES_USER:?POSTGRES_USER is not set in ${instance_env}}"
    : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set in ${instance_env}}"
    : "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set in ${instance_env}}"
    : "${SLOT_NAME:?SLOT_NAME is not set in ${instance_env}}"
    : "${PUBLICATION_NAME:?PUBLICATION_NAME is not set in ${instance_env}}"

    expected_name="$(basename "$instance_env" .env)"
    if [[ "$INSTANCE_NAME" != "$expected_name" ]]; then
      echo "ERROR: INSTANCE_NAME (${INSTANCE_NAME}) in ${instance_env} must match the filename (${expected_name}.env)." >&2
      exit 1
    fi

    CONNECTOR_NAME="postgres-source-${INSTANCE_NAME}"

    echo "==> [${INSTANCE_NAME}] Ensuring heartbeat table exists in Postgres ..."
    # Required by heartbeat.action.query in postgres-source.json.
    # The table must be in the publication so pgoutput forwards its WAL records
    # to Debezium, allowing confirmed_flush_lsn to advance even when watched
    # tables are idle (e.g. if the WAL backlog is from non-published schemas).
    PGPASSWORD="$POSTGRES_PASSWORD" docker run --rm -e PGPASSWORD \
      postgres:16-alpine \
      psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DBNAME" <<SQL
CREATE TABLE IF NOT EXISTS cdc.debezium_heartbeat (
  id  INT PRIMARY KEY DEFAULT 1,
  ts  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO cdc.debezium_heartbeat (id, ts) VALUES (1, now())
  ON CONFLICT (id) DO NOTHING;
SQL
    echo "    [${INSTANCE_NAME}] Heartbeat table ready."

    echo "==> [${INSTANCE_NAME}] Filling in connector config ..."
    PAYLOAD=$(jq \
      --arg host "$POSTGRES_HOST" \
      --arg port "$POSTGRES_PORT" \
      --arg user "$POSTGRES_USER" \
      --arg dbname "$POSTGRES_DBNAME" \
      --arg slot "$SLOT_NAME" \
      --arg pub "$PUBLICATION_NAME" \
      --arg inst "$INSTANCE_NAME" \
      '.name = "postgres-source-\($inst)"
       | .config["database.hostname"] = $host
       | .config["database.port"] = $port
       | .config["database.user"] = $user
       | .config["database.password"] = "${directory:/mnt/keyvault:cdc-\($inst)-postgres-password}"
       | .config["database.dbname"] = $dbname
       | .config["slot.name"] = $slot
       | .config["publication.name"] = $pub
       | .config["topic.prefix"] = "cdc-\($inst)"' \
      "$CONNECTOR_FILE")

    echo "==> [${INSTANCE_NAME}] Deploying ${CONNECTOR_NAME} ..."
    RESPONSE_FILE="$(mktemp)"
    trap 'rm -f "$RESPONSE_FILE"' RETURN
    HTTP_CODE=$(curl -s -o "$RESPONSE_FILE" -w "%{http_code}" \
      -X POST -H "Content-Type: application/json" \
      --data "$PAYLOAD" \
      "${CONNECT_URL}/connectors")

    if [[ "$HTTP_CODE" == "409" ]]; then
      echo "    [${INSTANCE_NAME}] Connector already exists -- continuing to monitor it."
    elif [[ "$HTTP_CODE" != "201" ]]; then
      echo "ERROR: [${INSTANCE_NAME}] deploy failed (HTTP ${HTTP_CODE})" >&2
      cat "$RESPONSE_FILE" >&2
      exit 1
    fi

    FINAL_STATE=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq -r '.connector.state // "UNKNOWN"')
    if [[ "$FINAL_STATE" != "RUNNING" ]]; then
      echo "ERROR: [${INSTANCE_NAME}] connector is not RUNNING (state=${FINAL_STATE})." >&2
      exit 1
    fi

    echo "==> [${INSTANCE_NAME}] ${CONNECTOR_NAME} is RUNNING. Streaming changes from cutover LSN -- no snapshot taken."
  )
}

echo "==> Waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
connect_up=0
for _ in $(seq 1 60); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    connect_up=1
    break
  fi
  sleep 5
done
if [[ "$connect_up" -ne 1 ]]; then
  echo "ERROR: Kafka Connect did not become healthy within 5 minutes." >&2
  exit 1
fi
echo "    Kafka Connect REST API is up."

if [[ $# -eq 1 ]]; then
  TARGET="${INSTANCES_DIR}/$1.env"
  [[ -f "$TARGET" ]] || { echo "ERROR: ${TARGET} not found -- copy instances/example.env.example to instances/$1.env first." >&2; exit 1; }
  deploy_one "$TARGET"
elif [[ $# -eq 0 ]]; then
  shopt -s nullglob
  FILES=("${INSTANCES_DIR}"/*.env)
  shopt -u nullglob
  if [[ ${#FILES[@]} -eq 0 ]]; then
    echo "ERROR: no instance env files found in ${INSTANCES_DIR} -- copy instances/example.env.example to instances/<name>.env first." >&2
    exit 1
  fi
  for f in "${FILES[@]}"; do
    deploy_one "$f"
  done
else
  echo "Usage: $0 [instance_name]" >&2
  exit 1
fi

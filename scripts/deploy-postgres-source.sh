#!/usr/bin/env bash
# Deploys the Debezium Postgres source connector (connectors/postgres-source.json).
# Run this BEFORE cutover -- the initial snapshot must finish while pg_chameleon
# is still the system of record. See README "Pre-cutover setup".
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"
CONNECTOR_FILE="${ROOT_DIR}/connectors/postgres-source.json"
CONNECTOR_NAME="postgres-source-connector"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${CONNECT_URL:=http://localhost:8083}"
: "${POSTGRES_HOST:?POSTGRES_HOST is not set -- copy .env.example to .env and fill it in}"
: "${POSTGRES_PORT:?POSTGRES_PORT is not set}"
: "${POSTGRES_USER:?POSTGRES_USER is not set}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set}"
: "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set}"
: "${POSTGRES_TABLE_LIST:?POSTGRES_TABLE_LIST is not set}"
: "${SLOT_NAME:?SLOT_NAME is not set}"
: "${PUBLICATION_NAME:?PUBLICATION_NAME is not set}"

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

echo "==> Filling in connector config from .env ..."
PAYLOAD=$(jq \
  --arg host "$POSTGRES_HOST" \
  --arg port "$POSTGRES_PORT" \
  --arg user "$POSTGRES_USER" \
  --arg password "$POSTGRES_PASSWORD" \
  --arg dbname "$POSTGRES_DBNAME" \
  --arg tables "$POSTGRES_TABLE_LIST" \
  --arg slot "$SLOT_NAME" \
  --arg pub "$PUBLICATION_NAME" \
  '.config["database.hostname"] = $host
   | .config["database.port"] = $port
   | .config["database.user"] = $user
   | .config["database.password"] = $password
   | .config["database.dbname"] = $dbname
   | .config["table.include.list"] = $tables
   | .config["slot.name"] = $slot
   | .config["publication.name"] = $pub' \
  "$CONNECTOR_FILE")

echo "==> Deploying ${CONNECTOR_NAME} ..."
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT
HTTP_CODE=$(curl -s -o "$RESPONSE_FILE" -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data "$PAYLOAD" \
  "${CONNECT_URL}/connectors")

if [[ "$HTTP_CODE" == "409" ]]; then
  echo "    Connector already exists -- continuing to monitor it."
elif [[ "$HTTP_CODE" != "201" ]]; then
  echo "ERROR: deploy failed (HTTP ${HTTP_CODE})" >&2
  cat "$RESPONSE_FILE" >&2
  exit 1
fi

echo "==> Monitoring initial snapshot progress ..."
echo "    (Looking for the snapshot-complete line in kafka-connect logs. This can take a while for large tables.)"
snapshot_done=0
for _ in $(seq 1 360); do
  STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || echo '{}')
  STATE=$(echo "$STATUS_JSON" | jq -r '.connector.state // "UNKNOWN"')
  TASK_STATE=$(echo "$STATUS_JSON" | jq -r '.tasks[0].state // "UNKNOWN"')
  echo "[$(date '+%H:%M:%S')] connector=${STATE} task=${TASK_STATE}"

  if [[ "$TASK_STATE" == "FAILED" ]]; then
    echo "ERROR: connector task FAILED. Full status:" >&2
    echo "$STATUS_JSON" | jq . >&2
    exit 1
  fi

  if docker compose -f "${ROOT_DIR}/docker-compose.yml" logs kafka-connect 2>/dev/null \
      | grep -q "Snapshot ended with SnapshotResult"; then
    snapshot_done=1
    echo "    Initial snapshot completed."
    break
  fi

  sleep 5
done

if [[ "$snapshot_done" -ne 1 ]]; then
  echo "WARNING: did not see a snapshot-complete log line within the timeout." >&2
  echo "          Check manually with: docker compose logs kafka-connect | grep -i snapshot" >&2
  echo "          Do NOT proceed with cutover until the snapshot is confirmed complete." >&2
fi

FINAL_STATE=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq -r '.connector.state // "UNKNOWN"')
if [[ "$FINAL_STATE" != "RUNNING" ]]; then
  echo "ERROR: connector is not RUNNING (state=${FINAL_STATE})." >&2
  exit 1
fi

echo "==> ${CONNECTOR_NAME} is RUNNING."
if [[ "$snapshot_done" -eq 1 ]]; then
  echo "==> Snapshot confirmed complete. Safe to proceed with cutover (see README 'Cutover procedure')."
fi

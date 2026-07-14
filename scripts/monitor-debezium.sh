#!/usr/bin/env bash
# Health check for the CDC rollback pipeline. Designed to run unattended,
# e.g. from cron, during the rollback window. Exits non-zero if anything
# needs attention so it can also be used as a simple alerting trigger.
#
#   */15 * * * * /path/to/scripts/monitor-debezium.sh >> /var/log/cdc-monitor.log 2>&1
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${CONNECT_URL:=http://localhost:8083}"
: "${WAL_THRESHOLD_MB:=5000}"
: "${POSTGRES_HOST:?POSTGRES_HOST is not set -- check .env}"
: "${POSTGRES_PORT:?POSTGRES_PORT is not set}"
: "${POSTGRES_USER:?POSTGRES_USER is not set}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set}"
: "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set}"
: "${SLOT_NAME:?SLOT_NAME is not set}"
: "${INSTANCE_NAME:?INSTANCE_NAME is not set -- check .env}"

SOURCE_CONNECTOR="postgres-source-${INSTANCE_NAME}"
OK=1
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=== CDC rollback pipeline health check: $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

# --- 1. Debezium source connector status ----------------------------------
STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${SOURCE_CONNECTOR}/status" 2>/dev/null || echo '')
if [[ -z "$STATUS_JSON" ]]; then
  echo -e "${RED}[FAIL]${NC} Could not reach Kafka Connect REST API or connector '${SOURCE_CONNECTOR}' not found."
  OK=0
else
  CONNECTOR_STATE=$(echo "$STATUS_JSON" | jq -r '.connector.state // "UNKNOWN"')
  TASK_STATE=$(echo "$STATUS_JSON" | jq -r '.tasks[0].state // "UNKNOWN"')
  if [[ "$CONNECTOR_STATE" == "RUNNING" && "$TASK_STATE" == "RUNNING" ]]; then
    echo -e "${GREEN}[OK]${NC}   ${SOURCE_CONNECTOR}: connector=${CONNECTOR_STATE} task=${TASK_STATE}"
  else
    echo -e "${RED}[FAIL]${NC} ${SOURCE_CONNECTOR}: connector=${CONNECTOR_STATE} task=${TASK_STATE}"
    OK=0
  fi
fi

# --- 2. Kafka consumer lag across all consumer groups ----------------------
GROUPS=$(docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 --list 2>/dev/null || true)

if [[ -z "$GROUPS" ]]; then
  echo -e "${YELLOW}[INFO]${NC} No active consumer groups (expected during normal operation -- the sink is idle)."
else
  while IFS= read -r GROUP; do
    [[ -z "$GROUP" ]] && continue
    DESC=$(docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka \
      kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null || true)
    TOTAL_LAG=$(echo "$DESC" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
    if [[ "$TOTAL_LAG" -gt 0 ]]; then
      echo -e "${YELLOW}[INFO]${NC} Consumer group '${GROUP}' lag: ${TOTAL_LAG} messages."
    else
      echo -e "${GREEN}[OK]${NC}   Consumer group '${GROUP}' lag: 0."
    fi
  done <<< "$GROUPS"
fi

# --- 3. Postgres replication slot WAL size ----------------------------------
SLOT_QUERY="SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}';"
SLOT_LAG_BYTES=$(PGPASSWORD="$POSTGRES_PASSWORD" docker run --rm -e PGPASSWORD \
  postgres:16-alpine \
  psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DBNAME" \
  -t -A -c "$SLOT_QUERY" 2>/dev/null | tr -d '[:space:]')

if [[ -z "$SLOT_LAG_BYTES" ]]; then
  echo -e "${RED}[FAIL]${NC} Could not read replication slot '${SLOT_NAME}' (slot missing, or Postgres unreachable)."
  OK=0
else
  SLOT_LAG_MB=$(( SLOT_LAG_BYTES / 1048576 ))
  if [[ "$SLOT_LAG_MB" -ge "$WAL_THRESHOLD_MB" ]]; then
    echo -e "${RED}[WARN]${NC} Replication slot '${SLOT_NAME}' WAL lag: ${SLOT_LAG_MB} MB (threshold: ${WAL_THRESHOLD_MB} MB)."
    echo -e "${RED}[WARN]${NC} Debezium is falling behind -- if this keeps growing, Postgres WAL disk usage grows unbounded until the slot is caught up or dropped."
    OK=0
  else
    echo -e "${GREEN}[OK]${NC}   Replication slot '${SLOT_NAME}' WAL lag: ${SLOT_LAG_MB} MB (threshold: ${WAL_THRESHOLD_MB} MB)."
  fi
fi

# --- Summary -----------------------------------------------------------------
echo
if [[ "$OK" -eq 1 ]]; then
  echo -e "${GREEN}STATUS: HEALTHY${NC}"
  exit 0
else
  echo -e "${RED}STATUS: ACTION REQUIRED${NC}"
  exit 1
fi

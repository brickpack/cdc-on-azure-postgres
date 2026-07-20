#!/usr/bin/env bash
# Health check for the CDC rollback pipeline. Designed to run unattended,
# e.g. from cron, during the rollback window. Exits non-zero if anything
# needs attention so it can also be used as a simple alerting trigger.
#
# Usage (Docker, on the VM next to the compose stack):
#   ./monitor-debezium.sh                       # check every instance under instances/
#   ./monitor-debezium.sh --instance billing    # check just one instance
#
# Usage (AKS, from any host with kubectl access, e.g. the jump host):
#   ./monitor-debezium.sh --mode aks --instance toolbox
#
# Docker mode reads Postgres details from instances/<name>.env.
# AKS mode reads host/user/db/slot from aks/values.local.yaml and the
# Postgres password from Key Vault (cdc-<name>-postgres-password); requires
# az login. AKS also swaps docker for kubectl (CDC_NAMESPACE default
# cdc-rollback; CDC_KAFKA_POD default cdc-kafka-broker-0) and needs
# CONNECT_URL to reach the Connect REST API, e.g. via:
#   kubectl port-forward -n cdc-rollback svc/cdc-connect-connect-api 8083:8083
#
#   */15 * * * * /path/to/scripts/monitor-debezium.sh --mode aks --instance toolbox \
#     >> /var/log/cdc-monitor.log 2>&1
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"
INSTANCES_DIR="${ROOT_DIR}/instances"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${CONNECT_URL:=http://localhost:8083}"
: "${WAL_THRESHOLD_MB:=5000}"

MODE="docker"        # docker | aks
INSTANCE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)     MODE="$2";     shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ "$MODE" != "docker" && "$MODE" != "aks" ]]; then
  echo "ERROR: --mode must be 'docker' or 'aks'" >&2
  exit 1
fi

NAMESPACE=""
KAFKA_POD=""
if [[ "$MODE" == "aks" ]]; then
  if [[ -z "$INSTANCE" ]]; then
    echo "ERROR: --instance <name> is required in AKS mode" >&2
    exit 1
  fi
  NAMESPACE="${CDC_NAMESPACE:-cdc-rollback}"
  KAFKA_POD="${CDC_KAFKA_POD:-cdc-kafka-broker-0}"
  # shellcheck source=lib/load-aks-instance.sh
  source "${SCRIPT_DIR}/lib/load-aks-instance.sh"
fi

# Run a psql query against the instance's Postgres server. Uses psql from
# PATH if available (the usual case on a jump host), otherwise falls back
# to docker run -- same convention as cdc-status.sh.
pg_query() {
  local sql="$1"
  if command -v psql &>/dev/null; then
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
      -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
      -U "$POSTGRES_USER" -d "$POSTGRES_DBNAME" \
      -t -A -c "$sql" 2>/dev/null | tr -d '[:space:]'
  else
    PGPASSWORD="$POSTGRES_PASSWORD" docker run --rm -e PGPASSWORD \
      postgres:16-alpine \
      psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
           -U "$POSTGRES_USER" -d "$POSTGRES_DBNAME" \
      -t -A -c "$sql" 2>/dev/null | tr -d '[:space:]'
  fi
}

# Run kafka-consumer-groups in the right environment. The Strimzi broker
# image ships the Kafka CLI as /opt/kafka/bin/*.sh; the Confluent image has
# kafka-consumer-groups on PATH.
consumer_groups() {
  if [[ "$MODE" == "docker" ]]; then
    docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka \
      kafka-consumer-groups "$@" </dev/null
  else
    kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
      /opt/kafka/bin/kafka-consumer-groups.sh "$@" </dev/null
  fi
}

INSTANCE_FILES=()
if [[ "$MODE" == "docker" ]]; then
  if [[ -n "$INSTANCE" ]]; then
    INSTANCE_FILES=("${INSTANCES_DIR}/${INSTANCE}.env")
    [[ -f "${INSTANCE_FILES[0]}" ]] || { echo "ERROR: ${INSTANCE_FILES[0]} not found." >&2; exit 1; }
  else
    shopt -s nullglob
    INSTANCE_FILES=("${INSTANCES_DIR}"/*.env)
    shopt -u nullglob
    if [[ ${#INSTANCE_FILES[@]} -eq 0 ]]; then
      echo "ERROR: no instance env files found in ${INSTANCES_DIR} -- copy instances/example.env.example to instances/<name>.env first." >&2
      exit 1
    fi
  fi
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

OVERALL_OK=1

echo "=== CDC rollback pipeline health check: $(date '+%Y-%m-%d %H:%M:%S %Z') ==="

# Runs connector + slot checks with POSTGRES_* / SLOT_NAME / INSTANCE_NAME already set.
check_instance_env() {
  (
    : "${POSTGRES_HOST:?POSTGRES_HOST is not set}"
    : "${POSTGRES_PORT:?POSTGRES_PORT is not set}"
    : "${POSTGRES_USER:?POSTGRES_USER is not set}"
    : "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set}"
    : "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set}"
    : "${SLOT_NAME:?SLOT_NAME is not set}"
    : "${INSTANCE_NAME:?INSTANCE_NAME is not set}"

    SOURCE_CONNECTOR="postgres-source-${INSTANCE_NAME}"
    INSTANCE_OK=1

    echo "--- Instance: ${INSTANCE_NAME} ---"

    # --- 1. Debezium source connector status --------------------------------
    STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${SOURCE_CONNECTOR}/status" 2>/dev/null || echo '')
    if [[ -z "$STATUS_JSON" ]]; then
      echo -e "${RED}[FAIL]${NC} Could not reach Kafka Connect REST API or connector '${SOURCE_CONNECTOR}' not found."
      INSTANCE_OK=0
    else
      CONNECTOR_STATE=$(echo "$STATUS_JSON" | jq -r '.connector.state // "UNKNOWN"')
      TASK_STATE=$(echo "$STATUS_JSON" | jq -r '.tasks[0].state // "UNKNOWN"')
      if [[ "$CONNECTOR_STATE" == "RUNNING" && "$TASK_STATE" == "RUNNING" ]]; then
        echo -e "${GREEN}[OK]${NC}   ${SOURCE_CONNECTOR}: connector=${CONNECTOR_STATE} task=${TASK_STATE}"
      else
        echo -e "${RED}[FAIL]${NC} ${SOURCE_CONNECTOR}: connector=${CONNECTOR_STATE} task=${TASK_STATE}"
        INSTANCE_OK=0
      fi
    fi

    # --- 2. Postgres replication slot WAL size -------------------------------
    SLOT_QUERY="SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}';"
    SLOT_LAG_BYTES=$(pg_query "$SLOT_QUERY")

    if [[ -z "$SLOT_LAG_BYTES" ]]; then
      echo -e "${RED}[FAIL]${NC} Could not read replication slot '${SLOT_NAME}' (slot missing, or Postgres unreachable)."
      INSTANCE_OK=0
    else
      SLOT_LAG_MB=$(( SLOT_LAG_BYTES / 1048576 ))
      if [[ "$SLOT_LAG_MB" -ge "$WAL_THRESHOLD_MB" ]]; then
        echo -e "${RED}[WARN]${NC} Replication slot '${SLOT_NAME}' WAL lag: ${SLOT_LAG_MB} MB (threshold: ${WAL_THRESHOLD_MB} MB)."
        echo -e "${RED}[WARN]${NC} Debezium is falling behind -- if this keeps growing, Postgres WAL disk usage grows unbounded until the slot is caught up or dropped."
        INSTANCE_OK=0
      else
        echo -e "${GREEN}[OK]${NC}   Replication slot '${SLOT_NAME}' WAL lag: ${SLOT_LAG_MB} MB (threshold: ${WAL_THRESHOLD_MB} MB)."
      fi
    fi

    exit $(( 1 - INSTANCE_OK ))
  )
}

check_instance_docker_env_file() {
  local instance_env="$1"
  (
    set -a
    # shellcheck disable=SC1090
    source "$instance_env"
    set +a
    check_instance_env
  )
}

if [[ "$MODE" == "aks" ]]; then
  load_aks_instance "$INSTANCE" || exit 1
  if ! check_instance_env; then
    OVERALL_OK=0
  fi
else
  for f in "${INSTANCE_FILES[@]}"; do
    if ! check_instance_docker_env_file "$f"; then
      OVERALL_OK=0
    fi
  done
fi

# --- Kafka consumer lag across all consumer groups (shared across instances) -
# (CONSUMER_GROUPS, not GROUPS: bash silently ignores assignments to GROUPS.)
CONSUMER_GROUPS=$(consumer_groups --bootstrap-server localhost:9092 --list 2>/dev/null || true)

if [[ -z "$CONSUMER_GROUPS" ]]; then
  echo -e "${YELLOW}[INFO]${NC} No active consumer groups (expected during normal operation -- sinks are idle)."
else
  while IFS= read -r GROUP; do
    [[ -z "$GROUP" ]] && continue
    DESC=$(consumer_groups --bootstrap-server localhost:9092 --describe --group "$GROUP" 2>/dev/null || true)
    TOTAL_LAG=$(echo "$DESC" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')
    if [[ "$TOTAL_LAG" -gt 0 ]]; then
      echo -e "${YELLOW}[INFO]${NC} Consumer group '${GROUP}' lag: ${TOTAL_LAG} messages."
    else
      echo -e "${GREEN}[OK]${NC}   Consumer group '${GROUP}' lag: 0."
    fi
  done <<< "$CONSUMER_GROUPS"
fi

# --- Summary -----------------------------------------------------------------
echo
if [[ "$OVERALL_OK" -eq 1 ]]; then
  echo -e "${GREEN}STATUS: HEALTHY${NC}"
  exit 0
else
  echo -e "${RED}STATUS: ACTION REQUIRED${NC}"
  exit 1
fi

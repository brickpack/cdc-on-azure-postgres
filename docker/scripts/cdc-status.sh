#!/usr/bin/env bash
# Full CDC pipeline status report (Docker Compose).
# Shows connector health, WAL lag, per-table operation counts, and any errors.
# Safe to run at any time -- read-only, no side effects.
#
# Usage:
#   ./scripts/cdc-status.sh                          # full report (only instance, if just one configured)
#   ./scripts/cdc-status.sh --instance billing       # full report for one instance (required if multiple configured)
#   ./scripts/cdc-status.sh --instance billing --tables               # topic totals only (fast)
#   ./scripts/cdc-status.sh --instance billing --tables --ops         # topic totals + INSERT/UPDATE/DELETE breakdown (slow)
#
# --ops reads every message in every topic to count operation types.
# With large topics (thousands of messages) this takes minutes.
#
# AKS equivalent: aks/scripts/cdc-status.sh
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

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
INSTANCE=""
TABLES_ONLY="false"
OPS_DETAIL="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)  INSTANCE="$2"; shift 2 ;;
    --tables)    TABLES_ONLY="true"; shift ;;
    --ops)       OPS_DETAIL="true"; shift ;;
    *)           echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Instance settings
# ---------------------------------------------------------------------------
: "${CONNECT_URL:=http://localhost:8083}"
: "${WAL_THRESHOLD_MB:=5000}"

INSTANCES_DIR="${ROOT_DIR}/instances"
if [[ -n "$INSTANCE" ]]; then
  INSTANCE_ENV_FILE="${INSTANCES_DIR}/${INSTANCE}.env"
  [[ -f "$INSTANCE_ENV_FILE" ]] || { echo "ERROR: ${INSTANCE_ENV_FILE} not found." >&2; exit 1; }
else
  shopt -s nullglob
  candidates=("${INSTANCES_DIR}"/*.env)
  shopt -u nullglob
  if [[ ${#candidates[@]} -eq 0 ]]; then
    echo "ERROR: no instance env files found in ${INSTANCES_DIR}. Copy instances/example.env.example to instances/<name>.env, or pass --instance <name>." >&2
    exit 1
  elif [[ ${#candidates[@]} -gt 1 ]]; then
    echo "ERROR: multiple instances configured -- pass --instance <name> to pick one:" >&2
    for c in "${candidates[@]}"; do echo "  $(basename "$c" .env)" >&2; done
    exit 1
  fi
  INSTANCE_ENV_FILE="${candidates[0]}"
fi

set -a
# shellcheck disable=SC1090
source "$INSTANCE_ENV_FILE"
set +a
: "${INSTANCE_NAME:?INSTANCE_NAME is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_HOST:?POSTGRES_HOST is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_PORT:?POSTGRES_PORT is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_USER:?POSTGRES_USER is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set in ${INSTANCE_ENV_FILE}}"
: "${SLOT_NAME:?SLOT_NAME is not set in ${INSTANCE_ENV_FILE}}"

TOPIC_PREFIX="cdc-${INSTANCE_NAME}"
SOURCE_CONNECTOR="postgres-source-${INSTANCE_NAME}"
SINK_CONNECTOR="mysql-rollback-sink-${INSTANCE_NAME}"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
INSTANCE="$INSTANCE_NAME"

# Wrapper: run a Kafka CLI command in the right environment
kafka_cmd() {
  docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka "$@" </dev/null
}

# Wrapper: tail kafka-connect logs
connect_logs() {
  docker compose -f "${ROOT_DIR}/docker-compose.yml" logs --since=1h kafka-connect 2>/dev/null
}

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

OK=1

header() { echo -e "\n${BOLD}${CYAN}=== $* ===${NC}"; }
ok()     { echo -e "  ${GREEN}[OK]${NC}    $*"; }
warn()   { echo -e "  ${YELLOW}[WARN]${NC}  $*"; OK=0; }
fail()   { echo -e "  ${RED}[FAIL]${NC}  $*"; OK=0; }
info()   { echo -e "  ${CYAN}[INFO]${NC}  $*"; }

# ---------------------------------------------------------------------------
# Helper: print connector status, highlight errors
# ---------------------------------------------------------------------------
check_connector() {
  local name="$1"
  local status_json
  status_json=$(curl -sf "${CONNECT_URL}/connectors/${name}/status" 2>/dev/null || echo '')

  if [[ -z "$status_json" ]]; then
    info "${name}: not deployed (skipping)"
    return
  fi

  local conn_state task_state trace
  conn_state=$(echo "$status_json" | jq -r '.connector.state // "UNKNOWN"')
  task_state=$(echo  "$status_json" | jq -r '.tasks[0].state  // "UNKNOWN"')
  trace=$(echo       "$status_json" | jq -r '.tasks[0].trace  // ""')

  if [[ "$conn_state" == "RUNNING" && "$task_state" == "RUNNING" ]]; then
    ok "${name}: connector=${conn_state} task=${task_state}"
  elif [[ "$task_state" == "FAILED" ]]; then
    fail "${name}: connector=${conn_state} task=${task_state}"
    if [[ -n "$trace" ]]; then
      # Print the root cause line only (last "Caused by:" or first line of trace)
      local cause
      cause=$(echo "$trace" | grep -m1 'Caused by:' || echo "$trace" | head -1)
      echo -e "           ${RED}${cause}${NC}"
    fi
  else
    warn "${name}: connector=${conn_state} task=${task_state}"
  fi
}

# ---------------------------------------------------------------------------
# Helper: run a psql query against the monitored Postgres server.
# Uses psql if available in PATH, otherwise falls back to docker run.
# ---------------------------------------------------------------------------
pg_query() {
  local sql="$1"
  if command -v psql &>/dev/null; then
    PGPASSWORD="$POSTGRES_PASSWORD" psql \
      -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
      -U "$POSTGRES_USER"  -d "$POSTGRES_DBNAME" \
      -t -A -c "$sql" 2>/dev/null | tr -d '[:space:]'
  else
    PGPASSWORD="$POSTGRES_PASSWORD" docker run --rm -e PGPASSWORD \
      postgres:16-alpine \
      psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
           -U "$POSTGRES_USER" -d "$POSTGRES_DBNAME" \
      -t -A -c "$sql" 2>/dev/null | tr -d '[:space:]'
  fi
}

# ---------------------------------------------------------------------------
# Helper: consumer group lag for a connector
# ---------------------------------------------------------------------------
show_lag() {
  local name="$1"
  local group="connect-${name}"
  local lag_output
  lag_output=$(kafka_cmd \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group "$group" 2>/dev/null || true)

  if [[ -z "$lag_output" ]]; then
    info "${name}: no active consumer group"
    return
  fi

  local total_lag
  total_lag=$(echo "$lag_output" | awk 'NR>1 && $6~/^[0-9]+$/ {sum+=$6} END {print sum+0}')
  if [[ "$total_lag" -gt 0 ]]; then
    warn "${name} consumer lag: ${total_lag} messages behind"
    echo "$lag_output" | awk 'NR==1 || ($6>0 && $6~/^[0-9]+$/)' | sed 's/^/           /'
  else
    ok "${name} consumer lag: 0 (fully caught up)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: WAL replication slot lag
# ---------------------------------------------------------------------------
check_wal_lag() {
  local slot_query="SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                    FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}';"
  local lag_bytes
  lag_bytes=$(pg_query "$slot_query")

  if [[ -z "$lag_bytes" ]]; then
    fail "Replication slot '${SLOT_NAME}' not found or Postgres unreachable"
    return
  fi

  local lag_mb=$(( lag_bytes / 1048576 ))
  if [[ "$lag_mb" -ge "$WAL_THRESHOLD_MB" ]]; then
    warn "WAL lag: ${lag_mb} MB (threshold: ${WAL_THRESHOLD_MB} MB) -- Debezium is falling behind"
  else
    ok "WAL lag: ${lag_mb} MB (threshold: ${WAL_THRESHOLD_MB} MB)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: recent errors from kafka-connect container logs
# ---------------------------------------------------------------------------
check_recent_errors() {
  local errors
  errors=$(connect_logs \
    | grep -E 'ERROR|Exception' | grep -v 'ssl\|SSL\|INFO' | tail -10 || true)

  if [[ -n "$errors" ]]; then
    warn "Recent errors in kafka-connect logs (last hour):"
    echo "$errors" | sed 's/^/           /' | head -10
  else
    ok "No ERROR lines in kafka-connect logs (last hour)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: heartbeat freshness
# ---------------------------------------------------------------------------
check_heartbeat() {
  local age_query="SELECT EXTRACT(EPOCH FROM (now() - ts))::int
                   FROM cdc.debezium_heartbeat WHERE id = 1;"
  local age_sec
  age_sec=$(pg_query "$age_query")

  if [[ -z "$age_sec" ]]; then
    warn "Heartbeat table not found -- heartbeat.action.query may not be working"
  elif [[ "$age_sec" -gt 60 ]]; then
    warn "Heartbeat last updated ${age_sec}s ago -- Debezium may be stalled (expected ≤10s)"
  else
    ok "Heartbeat last updated ${age_sec}s ago"
  fi
}

# ---------------------------------------------------------------------------
# Table operation breakdown
# ---------------------------------------------------------------------------
show_table_stats() {
  if [[ "$OPS_DETAIL" == "true" ]]; then
    header "KAFKA TOPIC OPERATION COUNTS (INSERT/UPDATE/DELETE)"
  else
    header "KAFKA TOPIC MESSAGE COUNTS"
  fi
  echo

  local topics
  topics=$(kafka_cmd \
    kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep "^${TOPIC_PREFIX}\." | grep -v '__debezium\|debezium_heartbeat' | sort)

  if [[ -z "$topics" ]]; then
    info "No CDC topics found -- no data captured yet"
    return
  fi

  if [[ "$OPS_DETAIL" == "true" ]]; then
    printf "  %-42s %8s %8s %8s %8s\n" "TABLE (schema.table)" "INSERT" "UPDATE" "DELETE" "TOTAL"
    printf "  %-42s %8s %8s %8s %8s\n" \
      "$(printf '%.0s-' {1..42})" "------" "------" "------" "-----"
  else
    printf "  %-42s %8s\n" "TABLE (schema.table)" "TOTAL"
    printf "  %-42s %8s\n" "$(printf '%.0s-' {1..42})" "-----"
  fi

  local grand_inserts=0 grand_updates=0 grand_deletes=0 grand_total=0

  while IFS= read -r topic; do
    local latest earliest total
    latest=$(kafka_cmd \
      kafka-run-class kafka.tools.GetOffsetShell \
      --bootstrap-server localhost:9092 --topic "$topic" --time -1 2>/dev/null \
      | awk -F: '{s+=$3} END {print s+0}')
    earliest=$(kafka_cmd \
      kafka-run-class kafka.tools.GetOffsetShell \
      --bootstrap-server localhost:9092 --topic "$topic" --time -2 2>/dev/null \
      | awk -F: '{s+=$3} END {print s+0}')
    total=$(( latest - earliest ))

    if [[ "$OPS_DETAIL" != "true" ]]; then
      printf "  %-42s %8s\n" "${topic#${TOPIC_PREFIX}.}" "$total"
      (( grand_total += total ))
      continue
    fi

    if [[ $total -eq 0 ]]; then
      printf "  %-42s %8s %8s %8s %8s\n" "${topic#${TOPIC_PREFIX}.}" "-" "-" "-" "0"
      continue
    fi

    local inserts=0 updates=0 deletes=0
    read -r inserts updates deletes < <(
      kafka_cmd kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic "$topic" \
        --from-beginning \
        --max-messages "$total" \
        --timeout-ms 20000 \
        --property print.value=true 2>/dev/null \
      | awk '
          /^\s*$/ || /^null$/ { d++ }   # null value = delete tombstone
          /"op":"c"/            { c++ }
          /"op":"u"/            { u++ }
          END { print c+0, u+0, d+0 }
        '
    )

    printf "  %-42s %8s %8s %8s %8s\n" \
      "${topic#${TOPIC_PREFIX}.}" "$inserts" "$updates" "$deletes" "$total"

    (( grand_inserts += inserts ))
    (( grand_updates += updates ))
    (( grand_deletes += deletes ))
    (( grand_total   += total   ))
  done <<< "$topics"

  if [[ "$OPS_DETAIL" == "true" ]]; then
    printf "  %-42s %8s %8s %8s %8s\n" \
      "$(printf '%.0s-' {1..42})" "------" "------" "------" "-----"
    printf "  %-42s %8s %8s %8s %8s\n" \
      "TOTAL" "$grand_inserts" "$grand_updates" "$grand_deletes" "$grand_total"
  else
    printf "  %-42s %8s\n" "$(printf '%.0s-' {1..42})" "-----"
    printf "  %-42s %8s\n" "TOTAL" "$grand_total"
  fi
}

# ===========================================================================
# Main
# ===========================================================================
echo -e "${BOLD}CDC Pipeline Status Report — $(date '+%Y-%m-%d %H:%M:%S %Z') (docker: ${INSTANCE})${NC}"

if [[ "$TABLES_ONLY" != "true" ]]; then
  header "CONNECTOR HEALTH"
  check_connector "$SOURCE_CONNECTOR"
  check_connector "$SINK_CONNECTOR"

  header "WAL REPLICATION SLOT"
  check_wal_lag
  check_heartbeat

  header "CONSUMER GROUP LAG"
  show_lag "$SOURCE_CONNECTOR"
  show_lag "$SINK_CONNECTOR"

  header "KAFKA-CONNECT LOG ERRORS (last hour)"
  check_recent_errors
fi

show_table_stats

echo
if [[ "$OK" -eq 1 ]]; then
  echo -e "${GREEN}${BOLD}OVERALL STATUS: HEALTHY${NC}"
else
  echo -e "${RED}${BOLD}OVERALL STATUS: ACTION REQUIRED${NC}"
fi
echo
exit $(( 1 - OK ))

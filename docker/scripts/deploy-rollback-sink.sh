#!/usr/bin/env bash
# EMERGENCY USE ONLY.
#
# Points the idle JDBC sink at the original source MySQL for ONE instance and
# starts replaying every change queued in that instance's Kafka topics since
# cutover. Only run this if rollback has actually been decided -- see README
# "Rollback procedure" for the full checklist (stop app writes FIRST, this
# script does not do that for you). Other instances keep capturing changes,
# unaffected.
#
# Usage:
#   ./deploy-rollback-sink.sh <instance_name> <mysql_host> <mysql_port> <mysql_user> <mysql_password> <mysql_dbname> [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"
INSTANCES_DIR="${ROOT_DIR}/instances"
SINK_FILE="${ROOT_DIR}/connectors/mysql-sink-standby.json"
TRANSFORMS_FILE="${ROOT_DIR}/connectors/type-transforms.json"

usage() {
  echo "Usage: $0 <instance_name> <mysql_host> <mysql_port> <mysql_user> <mysql_password> <mysql_dbname> [--yes]" >&2
  echo "  instance_name: matches instances/<instance_name>.env" >&2
  echo "  mysql_host/port/user/password/dbname: connection details for that instance's ORIGINAL source MySQL." >&2
  echo "  --yes: skip the interactive confirmation prompt (for use from another controlled script)." >&2
}

if [[ $# -lt 6 ]]; then
  usage
  exit 1
fi

INSTANCE="$1"
MYSQL_HOST="$2"
MYSQL_PORT="$3"
MYSQL_USER="$4"
MYSQL_PASSWORD="$5"
MYSQL_DBNAME="$6"
AUTO_YES="${7:-}"

INSTANCE_ENV_FILE="${INSTANCES_DIR}/${INSTANCE}.env"
[[ -f "$INSTANCE_ENV_FILE" ]] || { echo "ERROR: ${INSTANCE_ENV_FILE} not found." >&2; exit 1; }

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi
: "${CONNECT_URL:=http://localhost:8083}"

set -a
# shellcheck disable=SC1090
source "$INSTANCE_ENV_FILE"
set +a
: "${INSTANCE_NAME:?INSTANCE_NAME is not set in ${INSTANCE_ENV_FILE}}"
if [[ "$INSTANCE_NAME" != "$INSTANCE" ]]; then
  echo "ERROR: INSTANCE_NAME (${INSTANCE_NAME}) in ${INSTANCE_ENV_FILE} must match the filename (${INSTANCE}.env)." >&2
  exit 1
fi

CONNECTOR_NAME="mysql-rollback-sink-${INSTANCE_NAME}"

echo "############################################################"
echo "#  ROLLBACK SINK DEPLOY -- THIS WILL WRITE TO ORIGINAL MYSQL #"
echo "############################################################"
echo "Instance     : ${INSTANCE_NAME}"
echo "Target MySQL : ${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DBNAME}"
echo "Connect URL  : ${CONNECT_URL}"
echo

if [[ "$AUTO_YES" != "--yes" ]]; then
  read -r -p "Type ROLLBACK to confirm and continue: " CONFIRM
  if [[ "$CONFIRM" != "ROLLBACK" ]]; then
    echo "Aborted -- no changes made." >&2
    exit 1
  fi
fi

echo "==> Checking Kafka Connect REST API ..."
if ! curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
  echo "ERROR: Kafka Connect REST API is not reachable at ${CONNECT_URL}." >&2
  exit 1
fi
echo "    OK."

echo "==> Writing MySQL password to Connect container secret mount ..."
# Mirrors the layout Key Vault CSI produces in AKS. The file lives on a
# tmpfs so it never touches the VM's disk; gone when the container stops.
# Named per instance so multiple instances' rollback secrets can coexist.
docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka-connect \
  bash -c "printf '%s' '${MYSQL_PASSWORD}' > /mnt/keyvault/cdc-${INSTANCE_NAME}-mysql-password"
echo "    Done."

echo "==> Merging connectors/type-transforms.json into connectors/mysql-sink-standby.json ..."
MERGED_CONFIG=$(jq -s '
  (.[1] | del(._meta)) as $transforms
  | .[0].config + $transforms
' "$SINK_FILE" "$TRANSFORMS_FILE")

echo "==> Applying original MySQL connection details ..."
PAYLOAD=$(jq -n \
  --arg name "$CONNECTOR_NAME" \
  --argjson config "$MERGED_CONFIG" \
  --arg url "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DBNAME}?useSSL=true&serverTimezone=UTC" \
  --arg user "$MYSQL_USER" \
  --arg inst "$INSTANCE_NAME" \
  '{name: $name, config: ($config + {
      "topics.regex": "cdc-\($inst)\\..*",
      "connection.url": $url,
      "connection.user": $user,
      "connection.password": "${directory:/mnt/keyvault:cdc-\($inst)-mysql-password}"
    })}')

echo "==> Deploying ${CONNECTOR_NAME} ..."
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"' EXIT
HTTP_CODE=$(curl -s -o "$RESPONSE_FILE" -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data "$PAYLOAD" \
  "${CONNECT_URL}/connectors")

if [[ "$HTTP_CODE" == "409" ]]; then
  echo "    Connector already exists -- updating its config instead."
  HTTP_CODE=$(curl -s -o "$RESPONSE_FILE" -w "%{http_code}" \
    -X PUT -H "Content-Type: application/json" \
    --data "$(echo "$PAYLOAD" | jq '.config')" \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config")
  if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "201" ]]; then
    echo "ERROR: config update failed (HTTP ${HTTP_CODE})" >&2
    cat "$RESPONSE_FILE" >&2
    exit 1
  fi
elif [[ "$HTTP_CODE" != "201" ]]; then
  echo "ERROR: deploy failed (HTTP ${HTTP_CODE})" >&2
  cat "$RESPONSE_FILE" >&2
  exit 1
fi
echo "    Deployed."

CONSUMER_GROUP="connect-${CONNECTOR_NAME}"
echo
echo "==> Monitoring replay progress. Press Ctrl+C to stop watching (replay keeps running)."
echo "    Watching connector state and consumer-group lag for group: ${CONSUMER_GROUP}"
echo

for i in $(seq 1 720); do
  STATUS_JSON=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || echo '{}')
  STATE=$(echo "$STATUS_JSON" | jq -r '.connector.state // "UNKNOWN"')
  TASK_STATE=$(echo "$STATUS_JSON" | jq -r '.tasks[0].state // "UNKNOWN"')

  LAG_OUTPUT=$(docker compose -f "${ROOT_DIR}/docker-compose.yml" exec -T kafka \
    kafka-consumer-groups --bootstrap-server localhost:9092 \
    --describe --group "$CONSUMER_GROUP" 2>/dev/null || true)
  TOTAL_LAG=$(echo "$LAG_OUTPUT" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')

  echo "[$(date '+%H:%M:%S')] connector=${STATE} task=${TASK_STATE} total_lag=${TOTAL_LAG}"

  if [[ "$TASK_STATE" == "FAILED" ]]; then
    echo "ERROR: sink task FAILED. Full status:" >&2
    echo "$STATUS_JSON" | jq . >&2
    echo "Replay has STOPPED. Investigate before deciding whether to retry or abandon rollback." >&2
    exit 1
  fi

  if [[ "$TOTAL_LAG" == "0" && "$i" -gt 1 ]]; then
    echo "    Consumer lag has reached zero."
    break
  fi

  sleep 10
done

echo
echo "############################################################"
echo "#                     NEXT STEPS                          #"
echo "############################################################"
cat <<EOF
1. Confirm consumer lag is 0 (see output above). If it never reached 0,
   re-run this script's monitoring loop or check manually:
     docker compose exec kafka kafka-consumer-groups \\
       --bootstrap-server localhost:9092 --describe --group ${CONSUMER_GROUP}

2. Run the row-count verification queries from README "Rollback procedure"
   against both Postgres and the original MySQL, for instance ${INSTANCE_NAME}.

3. Spot-check critical tables for data integrity (README has sample queries).

4. Flip application connection strings back to the original MySQL.

5. Verify the application is functioning against MySQL.

6. Once confirmed, run ./scripts/teardown.sh ${INSTANCE_NAME} to remove this
   instance's connectors. Other instances are unaffected.
EOF

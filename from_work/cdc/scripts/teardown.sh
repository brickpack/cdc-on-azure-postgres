#!/usr/bin/env bash
# Tears down connectors after a rollback is fully verified (or a single
# instance's pipeline is being decommissioned). Does NOT touch Postgres or
# MySQL -- those cleanup steps are printed below as reminders, since they're
# irreversible.
#
# Usage:
#   ./teardown.sh <instance_name>   # delete this instance's connectors only;
#                                    # Kafka + Connect keep running for other instances
#   ./teardown.sh --all             # full docker compose down -v -- stops
#                                    # Kafka + Connect for EVERY instance
set -euo pipefail

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

usage() {
  echo "Usage: $0 <instance_name>   -- delete one instance's connectors" >&2
  echo "       $0 --all             -- docker compose down -v (stops the whole shared stack)" >&2
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

if [[ "$1" == "--all" ]]; then
  echo "==> Bringing down Docker Compose stack (containers, network, volumes) ..."
  echo "    This stops Kafka + Kafka Connect for EVERY instance, not just one."
  docker compose -f "${ROOT_DIR}/docker-compose.yml" down -v

  echo
  echo "############################################################"
  echo "#                  MANUAL CLEANUP REQUIRED                #"
  echo "############################################################"
  cat <<EOF
1. Drop the Debezium replication slot on Postgres for EVERY instance that
   was running (it is NOT removed by 'docker compose down -v' -- slots live
   in Postgres, not in this stack). Run against each instance's
   POSTGRES_HOST/POSTGRES_DBNAME:

     SELECT pg_drop_replication_slot('SLOT_NAME');

   (SLOT_NAME is set per instance in instances/<name>.env.) If it fails with
   "replication slot is active", first confirm no Kafka Connect container is
   still running, then retry.

2. Do NOT decommission any original MySQL server until you have positively
   confirmed its rollback window is closed:
     - Either rollback was completed successfully and the application is
       verified running against MySQL again, with no further need for
       this pipeline, OR
     - the post-cutover period has passed with no rollback triggered and
       the team has formally signed off that MySQL is no longer needed.
   Decommissioning early removes your only fallback if a problem surfaces
   later.
EOF
  exit 0
fi

INSTANCE="$1"
INSTANCE_ENV_FILE="${INSTANCES_DIR}/${INSTANCE}.env"
[[ -f "$INSTANCE_ENV_FILE" ]] || { echo "ERROR: ${INSTANCE_ENV_FILE} not found." >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$INSTANCE_ENV_FILE"
set +a
: "${INSTANCE_NAME:?INSTANCE_NAME is not set in ${INSTANCE_ENV_FILE}}"
: "${SLOT_NAME:?SLOT_NAME is not set in ${INSTANCE_ENV_FILE}}"

SOURCE_CONNECTOR="postgres-source-${INSTANCE_NAME}"
SINK_CONNECTOR="mysql-rollback-sink-${INSTANCE_NAME}"

echo "==> Deleting connectors for instance '${INSTANCE_NAME}' (Kafka + Connect keep running for other instances) ..."
for connector in "$SOURCE_CONNECTOR" "$SINK_CONNECTOR"; do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE "${CONNECT_URL}/connectors/${connector}" || echo "000")
  case "$HTTP_CODE" in
    204) echo "    Deleted ${connector}." ;;
    404) echo "    ${connector} was not deployed -- skipping." ;;
    *)   echo "    WARNING: could not delete ${connector} (HTTP ${HTTP_CODE})." >&2 ;;
  esac
done

echo
echo "############################################################"
echo "#                  MANUAL CLEANUP REQUIRED                #"
echo "############################################################"
cat <<EOF
1. Drop the Debezium replication slot for this instance on Postgres (it is
   NOT removed by deleting the connector -- it lives in Postgres):

     SELECT pg_drop_replication_slot('${SLOT_NAME}');

   If it fails with "replication slot is active", first confirm the
   ${SOURCE_CONNECTOR} connector is really gone, then retry.

2. Do NOT decommission this instance's original MySQL server until you have
   positively confirmed the rollback window is closed:
     - Either rollback was completed successfully and the application is
       verified running against MySQL again, with no further need for
       this pipeline, OR
     - the post-cutover period has passed with no rollback triggered and
       the team has formally signed off that MySQL is no longer needed.
   Decommissioning early removes your only fallback if a problem surfaces
   later.

3. This instance's Kafka topics (cdc-${INSTANCE_NAME}.*) and secret files
   still exist -- they are cleaned up only by '$0 --all' (full stack
   teardown), which removes Kafka's data volume entirely.
EOF

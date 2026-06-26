#!/usr/bin/env bash
# Tears down the Docker stack after rollback is fully verified (or the
# pipeline is being decommissioned because the rollback window closed
# without needing it). Does NOT touch Postgres or MySQL -- those cleanup
# steps are printed below as reminders, since they're irreversible.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="${ROOT_DIR}/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

SLOT_NAME="${SLOT_NAME:-PLACEHOLDER_SLOT_NAME}"

echo "==> Bringing down Docker Compose stack (containers, network, volumes) ..."
docker compose -f "${ROOT_DIR}/docker-compose.yml" down -v

echo
echo "############################################################"
echo "#                  MANUAL CLEANUP REQUIRED                #"
echo "############################################################"
cat <<EOF
1. Drop the Debezium replication slot on Postgres (it is NOT removed by
   'docker compose down -v' -- it lives in Postgres, not in this stack).
   Run this against POSTGRES_HOST/POSTGRES_DBNAME:

     SELECT pg_drop_replication_slot('${SLOT_NAME}');

   If it fails with "replication slot is active", first confirm no
   Kafka Connect container is still running, then retry.

2. Do NOT decommission the original MySQL server until you have
   positively confirmed the rollback window is closed:
     - Either rollback was completed successfully and the application is
       verified running against MySQL again, with no further need for
       this pipeline, OR
     - the post-cutover period has passed with no rollback triggered and
       the team has formally signed off that MySQL is no longer needed.
   Decommissioning early removes your only fallback if a problem surfaces
   later.
EOF

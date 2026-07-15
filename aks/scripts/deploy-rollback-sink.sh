#!/usr/bin/env bash
# EMERGENCY USE ONLY.
#
# AKS/Helm equivalent of docker/scripts/deploy-rollback-sink.sh. Deploys the JDBC
# sink for ONE instance, pointed at that instance's original source MySQL
# (declared in aks/values.local.yaml; password read from Key Vault at
# runtime), and starts replaying every change queued in Kafka since cutover.
# Only run this if rollback has actually been decided -- see aks/README.md
# "Rollback procedure" (stop app writes FIRST, this script does not do that
# for you).
#
# Usage:
#   ./deploy-rollback-sink.sh <instance> [--yes]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AKS_DIR="$(dirname "$SCRIPT_DIR")"
CHART_DIR="${AKS_DIR}/chart"
NAMESPACE="${CDC_NAMESPACE:-cdc-rollback}"
RELEASE="${CDC_RELEASE:-cdc-rollback}"
KAFKA_POD="cdc-kafka-broker-0"

usage() {
  echo "Usage: $0 <instance> [--yes]" >&2
  echo "  instance: instance name from aks/values.local.yaml (e.g. billing)." >&2
  echo "  --yes: skip the interactive confirmation prompt (for use from another controlled script)." >&2
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

INSTANCE="$1"
AUTO_YES="${2:-}"

CONNECTOR_NAME="mysql-rollback-sink-${INSTANCE}"
CONSUMER_GROUP="connect-${CONNECTOR_NAME}"

echo "==> Looking up the rollback target for '${INSTANCE}' in the release values ..."
MYSQL_TARGET="$(helm get values "$RELEASE" -n "$NAMESPACE" -o json \
  | jq -r --arg n "$INSTANCE" \
    '.instances[]? | select(.name==$n) | .mysql | select(. != null) | "\(.host):\(.port)/\(.dbname) (user: \(.user))"')"
if [[ -z "$MYSQL_TARGET" ]]; then
  echo "ERROR: no instance '${INSTANCE}' with a mysql block in release '${RELEASE}' (namespace ${NAMESPACE})." >&2
  echo "       Known instances:" >&2
  helm get values "$RELEASE" -n "$NAMESPACE" -o json | jq -r '.instances[]?.name' >&2 || true
  exit 1
fi

echo
echo "#############################################################"
echo "#  ROLLBACK SINK DEPLOY -- THIS WILL WRITE TO ORIGINAL MYSQL #"
echo "#############################################################"
echo "Instance     : ${INSTANCE}"
echo "Target MySQL : ${MYSQL_TARGET}"
echo "Helm release : ${RELEASE} (namespace ${NAMESPACE})"
echo

if [[ "$AUTO_YES" != "--yes" ]]; then
  read -r -p "Type ROLLBACK to confirm and continue: " CONFIRM
  if [[ "$CONFIRM" != "ROLLBACK" ]]; then
    echo "Aborted -- no changes made." >&2
    exit 1
  fi
fi

echo "==> Deploying ${CONNECTOR_NAME} via helm upgrade ..."
helm upgrade "$RELEASE" "$CHART_DIR" -n "$NAMESPACE" --reuse-values --set "rollback={${INSTANCE}}"
echo "    Deployed."

echo
echo "==> Monitoring replay progress. Press Ctrl+C to stop watching (replay keeps running)."
echo "    Watching connector state and consumer-group lag for group: ${CONSUMER_GROUP}"
echo

for i in $(seq 1 720); do
  STATE="$(kubectl get kafkaconnector "$CONNECTOR_NAME" -n "$NAMESPACE" \
    -o jsonpath='{.status.connectorStatus.connector.state}' 2>/dev/null || true)"
  TASK_STATE="$(kubectl get kafkaconnector "$CONNECTOR_NAME" -n "$NAMESPACE" \
    -o jsonpath='{.status.connectorStatus.tasks[0].state}' 2>/dev/null || true)"

  LAG_OUTPUT="$(kubectl exec -n "$NAMESPACE" "$KAFKA_POD" -c kafka -- \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "$CONSUMER_GROUP" 2>/dev/null || true)"
  TOTAL_LAG="$(echo "$LAG_OUTPUT" | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum+=$6} END {print sum+0}')"

  echo "[$(date '+%H:%M:%S')] connector=${STATE:-UNKNOWN} task=${TASK_STATE:-UNKNOWN} total_lag=${TOTAL_LAG}"

  if [[ "$TASK_STATE" == "FAILED" ]]; then
    echo "ERROR: sink task FAILED. Full status:" >&2
    kubectl get kafkaconnector "$CONNECTOR_NAME" -n "$NAMESPACE" -o yaml >&2
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
   check manually:
     kubectl exec -n ${NAMESPACE} ${KAFKA_POD} -c kafka -- \\
       /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \\
       --describe --group ${CONSUMER_GROUP}

2. Run the row-count verification queries from the main README "Rollback
   procedure" against both Postgres and the original MySQL.

3. Spot-check critical tables for data integrity.

4. Flip application connection strings back to the original MySQL.

5. Verify the application is functioning against MySQL.

6. Remove the sink:
     helm upgrade ${RELEASE} ${CHART_DIR} -n ${NAMESPACE} --reuse-values --set rollback=null
   Then follow aks/README.md "Post-rollback cleanup".
EOF

#!/usr/bin/env bash
# Builds the Strimzi-compatible Kafka Connect image from aks/Dockerfile.connect
# and pushes it to ACR. Run this before the first `helm install` and after
# any change to Dockerfile.connect or its plugin versions.
#
# NOT the same image as the Docker Compose stack: Strimzi needs its own
# base image and plugin layout (see Dockerfile.connect). Plugin versions
# are kept in sync with the Compose ../Dockerfile.
#
# Usage:
#   ./aks/scripts/build-push-connect-image.sh
#
# Override the image tag via environment variable:
#   CONNECT_IMAGE=myacr.azurecr.io/cdc-kafka-connect:3.9-cdc2 ./build-push-connect-image.sh
#
# The default tag is read from aks/values.local.yaml (connectImage field).
# If that file doesn't exist yet, set CONNECT_IMAGE explicitly.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AKS_DIR="$(dirname "$SCRIPT_DIR")"
VALUES_LOCAL="${AKS_DIR}/values.local.yaml"

# Resolve image tag: env override > values.local.yaml > error
if [[ -z "${CONNECT_IMAGE:-}" ]]; then
  if [[ ! -f "$VALUES_LOCAL" ]]; then
    echo "ERROR: CONNECT_IMAGE is not set and ${VALUES_LOCAL} does not exist." >&2
    echo "       Either set CONNECT_IMAGE or create values.local.yaml from values.example.yaml." >&2
    exit 1
  fi
  CONNECT_IMAGE="$(grep '^connectImage:' "$VALUES_LOCAL" | awk '{print $2}')"
  if [[ -z "$CONNECT_IMAGE" || "$CONNECT_IMAGE" == *PLACEHOLDER* ]]; then
    echo "ERROR: connectImage in ${VALUES_LOCAL} is not set or still a placeholder." >&2
    exit 1
  fi
fi

# Extract the registry hostname (everything before the first /)
ACR_HOST="${CONNECT_IMAGE%%/*}"

echo "==> Building ${CONNECT_IMAGE} from ${AKS_DIR}/Dockerfile.connect"
echo "    Platform: linux/amd64 (required for AKS; --platform flag ensures"
echo "    correct arch even when building on Apple Silicon)"
echo

docker buildx build \
  --platform linux/amd64 \
  --load \
  -f "${AKS_DIR}/Dockerfile.connect" \
  -t "$CONNECT_IMAGE" \
  "$AKS_DIR"

echo
echo "==> Logging in to ACR: ${ACR_HOST}"
az acr login -n "${ACR_HOST%%.*}"

echo
echo "==> Pushing ${CONNECT_IMAGE}"
docker push "$CONNECT_IMAGE"

echo
echo "==> Done. Update connectImage in aks/values.local.yaml if this is a new tag,"
echo "    then run: helm upgrade cdc-rollback aks/chart -n cdc-rollback -f aks/values.local.yaml"

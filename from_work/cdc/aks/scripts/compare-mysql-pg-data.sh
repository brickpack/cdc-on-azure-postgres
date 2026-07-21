#!/usr/bin/env bash
# AKS wrapper around migration/scripts/pg_chameleon/compare-mysql-pg-data.py.
#
# Loads one instance from aks/values.local.yaml and Key Vault (no passwords on
# the CLI), then runs the row-content compare. Postgres + MySQL are private —
# tunnel both through a jump host and override hosts/ports.
#
# Usage:
#   # Terminal A — Postgres tunnel
#   ssh -N -L 15432:<pg-fqdn>:5432 -i ~/.ssh/<key> <user>@<jump-ip>
#   # Terminal B — MySQL tunnel
#   ssh -N -L 13306:<mysql-fqdn>:3306 -i ~/.ssh/<key> <user>@<jump-ip>
#
#   POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
#   MYSQL_HOST=127.0.0.1 MYSQL_PORT=13306 \
#     ./aks/scripts/compare-mysql-pg-data.sh --instance toolbox
#
#   # Pass-through compare flags (see the Python script --help):
#   ... ./aks/scripts/compare-mysql-pg-data.sh --instance toolbox \
#         --table customers,orders --inspect 5 --pg-schema public
#
# Requires: az login, mysql client, psql, python3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AKS_DIR="$(dirname "$SCRIPT_DIR")"

# Locate the cdc/ tree (values + scripts/lib) and the shared compare .py.
resolve_cdc_root() {
  local d
  d="$(dirname "$AKS_DIR")"
  if [[ -f "${d}/scripts/lib/load-aks-instance.sh" && -d "${d}/aks" ]]; then
    (cd "$d" && pwd)
    return 0
  fi
  return 1
}

resolve_compare_py() {
  local cdc_root="$1"
  local candidate
  for candidate in \
    "${CDC_COMPARE_PY:-}" \
    "${cdc_root}/../migration/scripts/pg_chameleon/compare-mysql-pg-data.py" \
    "${cdc_root}/../../migration/scripts/pg_chameleon/compare-mysql-pg-data.py"
  do
    [[ -z "$candidate" ]] && continue
    if [[ -f "$candidate" ]]; then
      echo "$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")"
      return 0
    fi
  done
  return 1
}

usage() {
  cat <<'EOF'
Usage: ./aks/scripts/compare-mysql-pg-data.sh --instance <name> [compare options...]

Loads connection details from aks/values.local.yaml and passwords from Key Vault
(cdc-<name>-postgres-password, cdc-<name>-mysql-password). Remaining arguments
are forwarded to compare-mysql-pg-data.py (e.g. --table, --inspect, --pg-schema).

Tunnel overrides (env): POSTGRES_HOST POSTGRES_PORT MYSQL_HOST MYSQL_PORT
EOF
}

INSTANCE=""
FORWARD=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance)
      INSTANCE="${2:-}"
      shift 2
      ;;
    *)
      FORWARD+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$INSTANCE" ]]; then
  if [[ ${#FORWARD[@]} -eq 1 && ( "${FORWARD[0]}" == "-h" || "${FORWARD[0]}" == "--help" ) ]]; then
    usage
    exit 0
  fi
  echo "ERROR: --instance is required." >&2
  usage
  exit 1
fi

ROOT_DIR="$(resolve_cdc_root)" || {
  echo "ERROR: could not find cdc/ root (scripts/lib/load-aks-instance.sh)." >&2
  exit 1
}
COMPARE_PY="$(resolve_compare_py "$ROOT_DIR")" || {
  echo "ERROR: compare-mysql-pg-data.py not found under migration/scripts/pg_chameleon/." >&2
  echo "       Set CDC_COMPARE_PY to the full path if your checkout layout differs." >&2
  exit 1
}

# Prefer values next to this aks/ tree when present.
if [[ -z "${CDC_VALUES:-}" && -f "${AKS_DIR}/values.local.yaml" ]]; then
  export CDC_VALUES="${AKS_DIR}/values.local.yaml"
fi

# shellcheck source=../../scripts/lib/load-aks-instance.sh
source "${ROOT_DIR}/scripts/lib/load-aks-instance.sh"
load_aks_instance "$INSTANCE" || exit 1

if [[ -z "${MYSQL_HOST:-}" || -z "${MYSQL_PASSWORD:-}" ]]; then
  echo "ERROR: instance '${INSTANCE}' has no mysql: block (or mysql password missing in vault)." >&2
  echo "       Compare needs both Postgres and MySQL targets." >&2
  exit 1
fi

# Map AKS env names → variables expected by compare-mysql-pg-data.py
export MYSQL_FQDN="$MYSQL_HOST"
export MYSQL_PORT
export MYSQL_USER
export MYSQL_DB="$MYSQL_DBNAME"
export MYSQL_PASSWORD
export PG_FQDN="$POSTGRES_HOST"
export PG_PORT="$POSTGRES_PORT"
export PG_USER="$POSTGRES_USER"
export PG_DB="$POSTGRES_DBNAME"
export PG_PASSWORD="$POSTGRES_PASSWORD"
export PGSSLMODE="${PGSSLMODE:-require}"
: "${PG_SCHEMA:=public}"
export PG_SCHEMA
: "${LOG_DIR:=${HOME}/cdc/logs}"
export LOG_DIR

echo "==> Comparing MySQL ${MYSQL_FQDN}:${MYSQL_PORT}/${MYSQL_DB} (user ${MYSQL_USER})"
echo "    vs Postgres ${PG_FQDN}:${PG_PORT}/${PG_DB} schema ${PG_SCHEMA} (user ${PG_USER})"
echo "    instance=${INSTANCE}  log_dir=${LOG_DIR}"
echo

exec python3 "$COMPARE_PY" "${FORWARD[@]}"

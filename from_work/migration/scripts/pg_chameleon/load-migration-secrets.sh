#!/usr/bin/env bash
# Load MySQL and PostgreSQL passwords from Azure Key Vault into the current shell.
#
# MUST be sourced -- it exports env vars into your shell:
#
#   source load-migration-secrets.sh \
#     --key-vault   <vault-name> \
#     --mysql-secret <mysql-password-secret> \
#     --pg-secret    <pg-password-secret>
#
# Exports MYSQL_PASSWORD and PG_PASSWORD. Every migration script reads these
# env vars before falling back to the ~/.*_pw files, so once sourced they all
# pick up the vault values with no further arguments. Nothing is written to disk.
#
# Auth uses your current Azure CLI login (on the VM: `az login --identity`).
# Reading a secret needs only the vault name + secret name -- no resource group.
#
# Flags (each has an env-var equivalent, used when the flag is omitted):
#   --key-vault    NAME   Key Vault name                 (env KEY_VAULT_NAME)
#   --mysql-secret NAME   MySQL password secret name     (env MYSQL_PASSWORD_SECRET)
#   --pg-secret    NAME   PostgreSQL password secret name(env PG_PASSWORD_SECRET)

# Refuse to run when executed instead of sourced -- exports would vanish with
# the subshell and the caller would silently get no passwords.
if ! (return 0 2>/dev/null); then
  echo "This script must be sourced, not executed:" >&2
  echo "  source ${0##*/} --key-vault <vault> --mysql-secret <name> --pg-secret <name>" >&2
  exit 1
fi

_load_migration_secrets() {
  local kv_name="${KEY_VAULT_NAME:-}"
  local mysql_secret="${MYSQL_PASSWORD_SECRET:-}"
  local pg_secret="${PG_PASSWORD_SECRET:-}"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --key-vault)    kv_name="${2:-}";      shift 2 ;;
      --mysql-secret) mysql_secret="${2:-}"; shift 2 ;;
      --pg-secret)    pg_secret="${2:-}";    shift 2 ;;
      -h|--help|help)
        sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
        return 0 ;;
      *)
        echo "Unknown argument: $1" >&2
        return 2 ;;
    esac
  done

  local missing=()
  [[ -n "$kv_name" ]]      || missing+=("--key-vault / KEY_VAULT_NAME")
  [[ -n "$mysql_secret" ]] || missing+=("--mysql-secret / MYSQL_PASSWORD_SECRET")
  [[ -n "$pg_secret" ]]    || missing+=("--pg-secret / PG_PASSWORD_SECRET")
  if [[ ${#missing[@]} -gt 0 ]]; then
    echo "Missing required value(s): ${missing[*]}" >&2
    return 2
  fi

  if ! command -v az >/dev/null 2>&1; then
    echo "ERROR: 'az' (Azure CLI) not found on PATH." >&2
    return 1
  fi

  local mp pp
  mp="$(az keyvault secret show --vault-name "$kv_name" --name "$mysql_secret" --query value -o tsv)" \
    || { echo "ERROR: could not read secret '$mysql_secret' from vault '$kv_name'." >&2; return 1; }
  pp="$(az keyvault secret show --vault-name "$kv_name" --name "$pg_secret" --query value -o tsv)" \
    || { echo "ERROR: could not read secret '$pg_secret' from vault '$kv_name'." >&2; return 1; }

  [[ -n "$mp" ]] || { echo "ERROR: MySQL secret '$mysql_secret' is empty in vault '$kv_name'." >&2; return 1; }
  [[ -n "$pp" ]] || { echo "ERROR: PostgreSQL secret '$pg_secret' is empty in vault '$kv_name'." >&2; return 1; }

  export MYSQL_PASSWORD="$mp"
  export PG_PASSWORD="$pp"
  echo "Exported MYSQL_PASSWORD and PG_PASSWORD from Key Vault '$kv_name'."
}

_load_migration_secrets "$@"

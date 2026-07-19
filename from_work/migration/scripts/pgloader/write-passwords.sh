#!/usr/bin/env bash
# Prompt for MySQL and PostgreSQL passwords; write mode-0600 files (no secrets in shell history).
# Run from your laptop or on the worker after Terraform deploy.
#
# Usage:
#   scripts/migration/write-passwords.sh
#
# On the worker:
#   bash /opt/migration/write-passwords.sh
#
# Optional: MYSQL_PASSWORD_FILE=… PG_PASSWORD_FILE=… (each file receives one line).
#
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || "${1:-}" == "help" ]]; then
  cat <<'EOF'
Usage: write-passwords.sh

  Prompt (hidden) for MySQL + PostgreSQL passwords → ~/.mysql_migration_pw, ~/.pg_migration_pw (mode 0600).

  migration_load_db_passwords picks these up when unset; see migration-secrets.example.
EOF
  exit 0
fi

if [[ -n "${1:-}" ]]; then
  echo "Usage: $(basename "$0")" >&2
  exit 1
fi

if [[ -z "${HOME:-}" ]]; then
  echo "HOME must be set." >&2
  exit 1
fi

read -r -s -p "MySQL password: " _mp
echo
read -r -s -p "PostgreSQL password: " _pp
echo
if [[ -z "${_mp}" || -z "${_pp}" ]]; then
  echo "Both passwords must be non-empty." >&2
  exit 1
fi

MYSQL_OUT="${MYSQL_PASSWORD_FILE:-${HOME}/.mysql_migration_pw}"
PG_OUT="${PG_PASSWORD_FILE:-${HOME}/.pg_migration_pw}"
umask 077
printf '%s\n' "${_mp}" >"${MYSQL_OUT}.tmp.$$"
mv "${MYSQL_OUT}.tmp.$$" "${MYSQL_OUT}"
chmod 600 "${MYSQL_OUT}"
printf '%s\n' "${_pp}" >"${PG_OUT}.tmp.$$"
mv "${PG_OUT}.tmp.$$" "${PG_OUT}"
chmod 600 "${PG_OUT}"
unset _mp _pp
echo "Wrote ${MYSQL_OUT} and ${PG_OUT} (mode 0600)."
echo "migration_load_db_passwords picks these up when unset; see migration-secrets.example."

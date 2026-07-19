#!/usr/bin/env bash
# Simulate DML load (INSERT/UPDATE/DELETE) against one Postgres instance.
#
# This script is intended for CDC validation: generate sustained table churn so
# Debezium + Kafka + rollback sink behavior can be observed under load.
#
# Usage:
#   ./scripts/simulate-postgres-load.sh <instance_name> [options]
#
# Options:
#   --batches N       Number of batches to run (default: 20)
#   --batch-size N    DML cycles per batch (default: 100)
#   --sleep-ms N      Sleep between cycles inside each batch (default: 0)
#   --duration-sec N  Run for up to N seconds (default: 0 = disabled)
#   --with-deletes    Enable delete traffic (disabled by default)
#   --db-user USER    Override DB user used by this script
#   --db-password PASS Override DB password used by this script (avoid shell history)
#   --db-password-env VAR Read DB password from environment variable VAR
#
# Example:
#   ./scripts/simulate-postgres-load.sh toolbox --batches 50 --batch-size 200 --sleep-ms 5
#   ./scripts/simulate-postgres-load.sh toolbox --duration-sec 1800 --batch-size 200 --with-deletes
#
# Notes:
# - Reads Postgres connection details from instances/<instance_name>.env
# - If LOADTEST_POSTGRES_USER / LOADTEST_POSTGRES_PASSWORD are set in that
#   instance env file, they are used by default for this script.
# - Uses local psql if available; otherwise runs psql from postgres:16-alpine
# - Uses best-effort SQL blocks so occasional row-level failures don't abort
#   the whole load test run.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
INSTANCES_DIR="${ROOT_DIR}/instances"

usage() {
  cat <<'EOF'
Usage: ./scripts/simulate-postgres-load.sh <instance_name> [options]

Options:
  --batches N       Number of batches to run (default: 20)
  --batch-size N    DML cycles per batch (default: 100)
  --sleep-ms N      Sleep between cycles inside each batch (default: 0)
  --duration-sec N  Run for up to N seconds (default: 0 = disabled)
  --with-deletes    Enable delete traffic (disabled by default)
  --db-user USER    Override DB user used by this script
  --db-password PASS Override DB password used by this script (avoid shell history)
  --db-password-env VAR Read DB password from environment variable VAR
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

INSTANCE_NAME="$1"
shift

BATCHES=20
BATCH_SIZE=100
SLEEP_MS=0
DURATION_SEC=0
WITH_DELETES=false
DB_USER_OVERRIDE=""
DB_PASSWORD_OVERRIDE=""
DB_PASSWORD_ENV_VAR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --batches)
      BATCHES="$2"
      shift 2
      ;;
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --sleep-ms)
      SLEEP_MS="$2"
      shift 2
      ;;
    --duration-sec)
      DURATION_SEC="$2"
      shift 2
      ;;
    --with-deletes)
      WITH_DELETES=true
      shift
      ;;
    --db-user)
      DB_USER_OVERRIDE="$2"
      shift 2
      ;;
    --db-password)
      DB_PASSWORD_OVERRIDE="$2"
      shift 2
      ;;
    --db-password-env)
      DB_PASSWORD_ENV_VAR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

is_nonneg_int='^[0-9]+$'
if ! [[ "$BATCHES" =~ $is_nonneg_int ]]; then
  echo "ERROR: --batches must be a non-negative integer." >&2
  exit 1
fi
if ! [[ "$BATCH_SIZE" =~ $is_nonneg_int ]]; then
  echo "ERROR: --batch-size must be a non-negative integer." >&2
  exit 1
fi
if ! [[ "$SLEEP_MS" =~ $is_nonneg_int ]]; then
  echo "ERROR: --sleep-ms must be a non-negative integer." >&2
  exit 1
fi
if ! [[ "$DURATION_SEC" =~ $is_nonneg_int ]]; then
  echo "ERROR: --duration-sec must be a non-negative integer." >&2
  exit 1
fi
if [[ "$BATCH_SIZE" == "0" ]]; then
  echo "Nothing to do: batch_size=${BATCH_SIZE}."
  exit 0
fi
if [[ "$BATCHES" == "0" && "$DURATION_SEC" == "0" ]]; then
  echo "Nothing to do: batches=${BATCHES}, duration_sec=${DURATION_SEC}."
  exit 0
fi

if [[ "$WITH_DELETES" == "true" ]]; then
  WITH_DELETES_SQL=1
else
  WITH_DELETES_SQL=0
fi

INSTANCE_ENV_FILE="${INSTANCES_DIR}/${INSTANCE_NAME}.env"
[[ -f "$INSTANCE_ENV_FILE" ]] || {
  echo "ERROR: ${INSTANCE_ENV_FILE} not found." >&2
  exit 1
}

set -a
# shellcheck disable=SC1090
source "$INSTANCE_ENV_FILE"
set +a

: "${POSTGRES_HOST:?POSTGRES_HOST is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_PORT:?POSTGRES_PORT is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_USER:?POSTGRES_USER is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is not set in ${INSTANCE_ENV_FILE}}"
: "${POSTGRES_DBNAME:?POSTGRES_DBNAME is not set in ${INSTANCE_ENV_FILE}}"

if [[ -n "$DB_PASSWORD_ENV_VAR" ]]; then
  if [[ -z "${!DB_PASSWORD_ENV_VAR:-}" ]]; then
    echo "ERROR: environment variable ${DB_PASSWORD_ENV_VAR} is not set or empty." >&2
    exit 1
  fi
  DB_PASSWORD_OVERRIDE="${!DB_PASSWORD_ENV_VAR}"
fi

DB_HOST="$POSTGRES_HOST"
DB_PORT="$POSTGRES_PORT"
DB_NAME="$POSTGRES_DBNAME"
DB_USER_DEFAULT="${LOADTEST_POSTGRES_USER:-$POSTGRES_USER}"
DB_PASSWORD_DEFAULT="${LOADTEST_POSTGRES_PASSWORD:-$POSTGRES_PASSWORD}"
DB_USER="${DB_USER_OVERRIDE:-$DB_USER_DEFAULT}"
DB_PASSWORD="${DB_PASSWORD_OVERRIDE:-$DB_PASSWORD_DEFAULT}"

run_psql() {
  local sql="$1"
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" psql \
      -h "$DB_HOST" \
      -p "$DB_PORT" \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 \
      -q \
      -c "$sql"
  else
    PGPASSWORD="$DB_PASSWORD" docker run --rm -e PGPASSWORD postgres:16-alpine \
      psql \
      -h "$DB_HOST" \
      -p "$DB_PORT" \
      -U "$DB_USER" \
      -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 \
      -q \
      -c "$sql"
  fi
}

echo "=========================================================="
echo "Postgres DML load simulation"
echo "Instance   : ${INSTANCE_NAME}"
echo "Database   : ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "DB User    : ${DB_USER}"
echo "Batches    : ${BATCHES}"
echo "Batch size : ${BATCH_SIZE}"
echo "Sleep/cycle: ${SLEEP_MS} ms"
if [[ "$DURATION_SEC" -gt 0 ]]; then
  echo "Duration   : ${DURATION_SEC} sec"
fi
echo "Deletes    : ${WITH_DELETES}"
if [[ "$DURATION_SEC" -eq 0 ]]; then
  echo "Total cycles: $((BATCHES * BATCH_SIZE))"
fi
echo "=========================================================="

start_epoch=$(date +%s)
batch=0
while true; do
  now_epoch=$(date +%s)
  elapsed=$((now_epoch - start_epoch))

  if [[ "$DURATION_SEC" -gt 0 && "$elapsed" -ge "$DURATION_SEC" ]]; then
    break
  fi

  if [[ "$BATCHES" -gt 0 && "$batch" -ge "$BATCHES" ]]; then
    break
  fi

  batch=$((batch + 1))
  if [[ "$BATCHES" -gt 0 ]]; then
    echo "==> Running batch ${batch}/${BATCHES}"
  else
    echo "==> Running batch ${batch} (duration mode)"
  fi

  read -r -d '' SQL_BLOCK <<SQL || true
DO \$\$
DECLARE
  i integer;
  v_domain_id bigint;
  v_subscription_id bigint;
  v_universal_key_id bigint;
  v_host_id bigint;
  v_uid_id bigint;
  v_profile_id bigint;
  v_delete_roll double precision;
  v_domain_min_id bigint;
  v_domain_max_id bigint;
  v_subscription_min_id bigint;
  v_subscription_max_id bigint;
  v_universal_key_min_id bigint;
  v_universal_key_max_id bigint;
  v_host_min_id bigint;
  v_host_max_id bigint;
  v_uid_min_id bigint;
  v_uid_max_id bigint;
  v_pick_id bigint;
  v_uid_seq text;
BEGIN
  -- Pre-compute id ranges once per batch to avoid ORDER BY random() full scans.
  SELECT min(id), max(id) INTO v_domain_min_id, v_domain_max_id FROM toolbox."domain";
  SELECT min(id), max(id) INTO v_subscription_min_id, v_subscription_max_id FROM toolbox."subscription";
  SELECT min(id), max(id) INTO v_universal_key_min_id, v_universal_key_max_id FROM toolbox.universal_key;
  SELECT min(id), max(id) INTO v_host_min_id, v_host_max_id FROM toolbox.host;
  SELECT min(id), max(id) INTO v_uid_min_id, v_uid_max_id FROM toolbox.user_in_domain;
  v_uid_seq := pg_get_serial_sequence('toolbox.user_in_domain', 'id');

  FOR i IN 1..${BATCH_SIZE} LOOP
    v_domain_id := NULL;
    v_subscription_id := NULL;
    v_universal_key_id := NULL;
    v_host_id := NULL;
    v_uid_id := NULL;

    -- INSERT: toolbox.domain
    BEGIN
      INSERT INTO toolbox."domain" (id, is_business, created_at, updated_at)
      VALUES (
        nextval('toolbox.domain_id_seq'::regclass),
        (random() < 0.5),
        now(),
        now()
      )
      RETURNING id INTO v_domain_id;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT: toolbox.subscription
    BEGIN
      INSERT INTO toolbox."subscription" (
        id, "uuid", expiration_date, expiration_date_deferred,
        autorenewable, deleted, "name", is_nfr, is_suspended, is_beta,
        created_at, updated_at, parent_id, activated_at, owner_id
      )
      VALUES (
        nextval('toolbox.subscription_id_seq'::regclass),
        md5(clock_timestamp()::text || random()::text),
        now() + (((random() * 365)::int)::text || ' days')::interval,
        NULL,
        (random() < 0.5),
        false,
        'load-sub-' || substr(md5(random()::text), 1, 10),
        false,
        false,
        false,
        now(),
        now(),
        NULL,
        now(),
        COALESCE((
          SELECT owner_id
          FROM toolbox.host
          WHERE owner_id IS NOT NULL
            AND id >= (floor(random() * GREATEST(v_host_max_id - v_host_min_id + 1, 1))::bigint + COALESCE(v_host_min_id, 1))
          ORDER BY id
          LIMIT 1
        ), 0)
      )
      RETURNING id INTO v_subscription_id;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT: toolbox.universal_key
    BEGIN
      IF v_subscription_id IS NOT NULL THEN
        INSERT INTO toolbox.universal_key (
          id, lic_key, is_blacklisted, subscription_id, created_at, updated_at
        )
        VALUES (
          nextval('toolbox.universal_key_id_seq'::regclass),
          'UK-' || substr(md5(clock_timestamp()::text || random()::text), 1, 20),
          false,
          v_subscription_id,
          now(),
          now()
        )
        RETURNING id INTO v_universal_key_id;
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT: toolbox.host
    BEGIN
      INSERT INTO toolbox.host (
        id, hw_id, "name", os_name, product_version,
        owner_id, activated_at, created_at, updated_at
      )
      VALUES (
        nextval('toolbox.host_id_seq'::regclass),
        'hw-' || substr(md5(clock_timestamp()::text || random()::text), 1, 12),
        'load-host-' || substr(md5(random()::text), 1, 8),
        'linux',
        '1.0.' || ((random() * 100)::int)::text,
        COALESCE((
          SELECT owner_id
          FROM toolbox.host
          WHERE owner_id IS NOT NULL
            AND id >= (floor(random() * GREATEST(v_host_max_id - v_host_min_id + 1, 1))::bigint + COALESCE(v_host_min_id, 1))
          ORDER BY id
          LIMIT 1
        ), 0),
        now(),
        now(),
        now()
      )
      RETURNING id INTO v_host_id;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT: toolbox.host_to_universal_key
    BEGIN
      IF v_host_id IS NOT NULL AND v_universal_key_id IS NOT NULL THEN
        INSERT INTO toolbox.host_to_universal_key (
          id, host_id, universal_key_id, created_at
        )
        VALUES (
          nextval('toolbox.host_to_universal_key_id_seq'::regclass),
          v_host_id,
          v_universal_key_id,
          now()
        );
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT: toolbox.user_in_domain
    BEGIN
      IF v_domain_id IS NOT NULL THEN
        IF v_uid_min_id IS NOT NULL AND v_uid_max_id IS NOT NULL THEN
          v_pick_id := floor(random() * (v_uid_max_id - v_uid_min_id + 1))::bigint + v_uid_min_id;
          SELECT profile_id
          INTO v_profile_id
          FROM toolbox.user_in_domain
          WHERE id >= v_pick_id
          ORDER BY id
          LIMIT 1;
        END IF;

        IF v_profile_id IS NOT NULL THEN
          IF v_uid_seq IS NOT NULL THEN
            EXECUTE format('SELECT nextval(%L)', v_uid_seq) INTO v_uid_id;
          ELSE
            SELECT COALESCE(max(id), 0) + 1 INTO v_uid_id FROM toolbox.user_in_domain;
          END IF;

          INSERT INTO toolbox.user_in_domain (
            id, profile_id, domain_id, is_admin, is_blocked, created_at, updated_at
          )
          VALUES (
            v_uid_id,
            v_profile_id,
            v_domain_id,
            false,
            false,
            now(),
            now()
          );
        END IF;
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- UPDATE activity across existing rows
    BEGIN
      IF v_domain_min_id IS NOT NULL AND v_domain_max_id IS NOT NULL THEN
        v_pick_id := floor(random() * (v_domain_max_id - v_domain_min_id + 1))::bigint + v_domain_min_id;
      END IF;
      UPDATE toolbox."domain"
      SET updated_at = now(), is_business = NOT is_business
      WHERE id = (
        SELECT id FROM toolbox."domain"
        WHERE id >= COALESCE(v_pick_id, 0)
        ORDER BY id LIMIT 1
      );
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_subscription_min_id IS NOT NULL AND v_subscription_max_id IS NOT NULL THEN
        v_pick_id := floor(random() * (v_subscription_max_id - v_subscription_min_id + 1))::bigint + v_subscription_min_id;
      END IF;
      UPDATE toolbox."subscription"
      SET updated_at = now(), is_suspended = NOT is_suspended
      WHERE id = (
        SELECT id FROM toolbox."subscription"
        WHERE id >= COALESCE(v_pick_id, 0)
        ORDER BY id LIMIT 1
      );
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_universal_key_min_id IS NOT NULL AND v_universal_key_max_id IS NOT NULL THEN
        v_pick_id := floor(random() * (v_universal_key_max_id - v_universal_key_min_id + 1))::bigint + v_universal_key_min_id;
      END IF;
      UPDATE toolbox.universal_key
      SET updated_at = now(), is_blacklisted = NOT is_blacklisted
      WHERE id = (
        SELECT id FROM toolbox.universal_key
        WHERE id >= COALESCE(v_pick_id, 0)
        ORDER BY id LIMIT 1
      );
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_host_min_id IS NOT NULL AND v_host_max_id IS NOT NULL THEN
        v_pick_id := floor(random() * (v_host_max_id - v_host_min_id + 1))::bigint + v_host_min_id;
      END IF;
      UPDATE toolbox.host
      SET updated_at = now(), product_version = '1.0.' || ((random() * 100)::int)::text
      WHERE id = (
        SELECT id FROM toolbox.host
        WHERE id >= COALESCE(v_pick_id, 0)
        ORDER BY id LIMIT 1
      );
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_uid_min_id IS NOT NULL AND v_uid_max_id IS NOT NULL THEN
        v_pick_id := floor(random() * (v_uid_max_id - v_uid_min_id + 1))::bigint + v_uid_min_id;
      END IF;
      UPDATE toolbox.user_in_domain
      SET updated_at = now(), is_blocked = NOT is_blocked
      WHERE id = (
        SELECT id FROM toolbox.user_in_domain
        WHERE id >= COALESCE(v_pick_id, 0)
        ORDER BY id LIMIT 1
      );
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- DELETE traffic is optional and disabled by default.
    IF ${WITH_DELETES_SQL} = 1 THEN
      v_delete_roll := random();
      IF v_delete_roll < 0.35 THEN
        BEGIN
          IF v_host_id IS NOT NULL THEN
            DELETE FROM toolbox.host_to_universal_key WHERE host_id = v_host_id;
          END IF;
          IF v_uid_id IS NOT NULL THEN
            DELETE FROM toolbox.user_in_domain WHERE id = v_uid_id;
          END IF;
          IF v_host_id IS NOT NULL THEN
            DELETE FROM toolbox.host WHERE id = v_host_id;
          END IF;
          IF v_universal_key_id IS NOT NULL THEN
            DELETE FROM toolbox.universal_key WHERE id = v_universal_key_id;
          END IF;
          IF v_subscription_id IS NOT NULL THEN
            DELETE FROM toolbox."subscription" WHERE id = v_subscription_id;
          END IF;
          IF v_domain_id IS NOT NULL THEN
            DELETE FROM toolbox."domain" WHERE id = v_domain_id;
          END IF;
        EXCEPTION WHEN others THEN
          NULL;
        END;
      END IF;
    END IF;

    IF ${SLEEP_MS} > 0 THEN
      PERFORM pg_sleep(${SLEEP_MS} / 1000.0);
    END IF;
  END LOOP;
END
\$\$;
SQL

  run_psql "$SQL_BLOCK"
  if [[ "$BATCHES" -gt 0 ]]; then
    echo "    Batch ${batch}/${BATCHES} complete"
  else
    echo "    Batch ${batch} complete"
  fi
done

echo
echo "Load simulation finished."
echo "Tip: check CDC lag/status with: ./scripts/cdc-status.sh --instance ${INSTANCE_NAME}"
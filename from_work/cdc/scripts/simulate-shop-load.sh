#!/usr/bin/env bash
# Simulate DML load against the shop demo schema (public.customers / products /
# orders / order_items) so Debezium + Kafka can be observed under traffic.
#
# Connection (AKS) — passwords never on the CLI:
#   Host/db from aks/values.local.yaml; passwords from Key Vault.
#   Writable role (recommended): set postgres.loadUser in values.local.yaml and
#   store its password as cdc-<name>-postgres-load-password in that instance's
#   vault. Falls back to postgres.user + cdc-<name>-postgres-password if
#   loadUser is unset (usually cdc_replication / SELECT-only — DML will fail).
#   Override host/port for a jump-host tunnel.
#
# Usage:
#   # One-time: writable password in vault + loadUser in values.local.yaml
#   #   az keyvault secret set --vault-name dbmig-dev-kv \
#   #     --name cdc-toolbox-postgres-load-password --file ./pass.txt
#   #   # values: postgres.loadUser: psqladmin
#   #
#   # Tunnel: ssh -N -L 15432:<pg-host>:5432 -i ~/.ssh/key azureuser@<jump>
#   POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
#     ./scripts/simulate-shop-load.sh --instance toolbox --batches 20 --batch-size 50
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

usage() {
  cat <<'EOF'
Usage: ./scripts/simulate-shop-load.sh --instance <name> [options]

Passwords come from Key Vault only (see script header). No password flags.

Options:
  --instance NAME     AKS instance name (required; matches values.local.yaml)
  --batches N         Number of batches (default: 20; set 0 with --duration-sec)
  --batch-size N      DML cycles per batch (default: 100)
  --sleep-ms N        Sleep between cycles inside each batch (default: 0)
  --duration-sec N    Stop after N seconds (default: 0 = disabled)
  --with-deletes      Delete ~35% of rows created in each cycle (off by default)
EOF
}

INSTANCE=""
BATCHES=20
BATCH_SIZE=100
SLEEP_MS=0
DURATION_SEC=0
WITH_DELETES=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --instance) INSTANCE="$2"; shift 2 ;;
    --batches) BATCHES="$2"; shift 2 ;;
    --batch-size) BATCH_SIZE="$2"; shift 2 ;;
    --sleep-ms) SLEEP_MS="$2"; shift 2 ;;
    --duration-sec) DURATION_SEC="$2"; shift 2 ;;
    --with-deletes) WITH_DELETES=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$INSTANCE" ]]; then
  echo "ERROR: --instance is required." >&2
  usage
  exit 1
fi

is_nonneg_int='^[0-9]+$'
for pair in "batches:$BATCHES" "batch-size:$BATCH_SIZE" "sleep-ms:$SLEEP_MS" "duration-sec:$DURATION_SEC"; do
  name="${pair%%:*}"
  val="${pair#*:}"
  if ! [[ "$val" =~ $is_nonneg_int ]]; then
    echo "ERROR: --${name} must be a non-negative integer." >&2
    exit 1
  fi
done
if [[ "$BATCH_SIZE" == "0" ]]; then
  echo "Nothing to do: batch_size=0."
  exit 0
fi
if [[ "$BATCHES" == "0" && "$DURATION_SEC" == "0" ]]; then
  echo "Nothing to do: batches=0 and duration_sec=0."
  exit 0
fi

# shellcheck source=lib/load-aks-instance.sh
source "${SCRIPT_DIR}/lib/load-aks-instance.sh"
load_aks_instance "$INSTANCE" || exit 1

DB_HOST="$POSTGRES_HOST"
DB_PORT="$POSTGRES_PORT"
DB_NAME="$POSTGRES_DBNAME"
if [[ -n "${POSTGRES_LOAD_USER:-}" && -n "${POSTGRES_LOAD_PASSWORD:-}" ]]; then
  DB_USER="$POSTGRES_LOAD_USER"
  DB_PASSWORD="$POSTGRES_LOAD_PASSWORD"
else
  DB_USER="$POSTGRES_USER"
  DB_PASSWORD="$POSTGRES_PASSWORD"
  if [[ "$DB_USER" == "cdc_replication" ]]; then
    echo "WARNING: no postgres.loadUser / cdc-${INSTANCE}-postgres-load-password;" >&2
    echo "         falling back to cdc_replication (usually SELECT-only)." >&2
  fi
fi
: "${PGSSLMODE:=require}"

WITH_DELETES_SQL=0
[[ "$WITH_DELETES" == "true" ]] && WITH_DELETES_SQL=1

run_psql() {
  local sql="$1"
  if command -v psql >/dev/null 2>&1; then
    PGPASSWORD="$DB_PASSWORD" PGSSLMODE="$PGSSLMODE" psql \
      -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 -q -c "$sql"
  else
    PGPASSWORD="$DB_PASSWORD" PGSSLMODE="$PGSSLMODE" docker run --rm \
      -e PGPASSWORD -e PGSSLMODE postgres:16-alpine \
      psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
      -v ON_ERROR_STOP=1 -q -c "$sql"
  fi
}

echo "=========================================================="
echo "Shop schema DML load simulation"
echo "Instance   : ${INSTANCE}"
echo "Database   : ${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo "DB User    : ${DB_USER}"
echo "Batches    : ${BATCHES}"
echo "Batch size : ${BATCH_SIZE}"
echo "Sleep/cycle: ${SLEEP_MS} ms"
[[ "$DURATION_SEC" -gt 0 ]] && echo "Duration   : ${DURATION_SEC} sec"
echo "Deletes    : ${WITH_DELETES}"
[[ "$DURATION_SEC" -eq 0 ]] && echo "Total cycles: $((BATCHES * BATCH_SIZE))"
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
  v_customer_id bigint;
  v_product_id bigint;
  v_order_id bigint;
  v_item_id bigint;
  v_pick_id bigint;
  v_cust_min bigint;
  v_cust_max bigint;
  v_prod_min bigint;
  v_prod_max bigint;
  v_ord_min bigint;
  v_ord_max bigint;
  v_status text;
  v_n_items integer;
  v_j integer;
  v_delete_roll double precision;
  v_statuses text[] := ARRAY['pending', 'paid', 'shipped', 'cancelled', 'refunded'];
BEGIN
  SELECT min(id), max(id) INTO v_cust_min, v_cust_max FROM public.customers;
  SELECT min(id), max(id) INTO v_prod_min, v_prod_max FROM public.products;
  SELECT min(id), max(id) INTO v_ord_min, v_ord_max FROM public.orders;

  FOR i IN 1..${BATCH_SIZE} LOOP
    v_customer_id := NULL;
    v_product_id := NULL;
    v_order_id := NULL;

    -- INSERT customer
    BEGIN
      INSERT INTO public.customers (
        id, first_name, last_name, email, is_active, created_at
      ) VALUES (
        nextval('customers_id_seq'::regclass),
        'Load',
        'Cust' || substr(md5(random()::text), 1, 6),
        'load-' || substr(md5(clock_timestamp()::text || random()::text), 1, 16) || '@example.test',
        true,
        now()
      )
      RETURNING id INTO v_customer_id;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT product (~50% of cycles; otherwise reuse an existing one)
    BEGIN
      IF random() < 0.5 OR v_prod_min IS NULL THEN
        INSERT INTO public.products (
          id, "name", description, price, stock, is_available, created_at
        ) VALUES (
          nextval('products_id_seq'::regclass),
          'Widget-' || substr(md5(random()::text), 1, 8),
          'load-test product',
          round((random() * 200 + 1)::numeric, 2),
          (random() * 500)::int,
          true,
          now()
        )
        RETURNING id INTO v_product_id;
        v_prod_min := COALESCE(v_prod_min, v_product_id);
        v_prod_max := GREATEST(COALESCE(v_prod_max, v_product_id), v_product_id);
      ELSE
        v_pick_id := floor(random() * (v_prod_max - v_prod_min + 1))::bigint + v_prod_min;
        SELECT id INTO v_product_id
        FROM public.products
        WHERE id >= v_pick_id
        ORDER BY id
        LIMIT 1;
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- INSERT order (+ 1–3 order_items)
    BEGIN
      IF v_customer_id IS NOT NULL THEN
        v_status := v_statuses[1 + floor(random() * array_length(v_statuses, 1))::int];
        INSERT INTO public.orders (
          id, customer_id, status, total_amount, notes, created_at
        ) VALUES (
          nextval('orders_id_seq'::regclass),
          v_customer_id,
          v_status,
          0,
          'load-test',
          now()
        )
        RETURNING id INTO v_order_id;

        v_n_items := 1 + floor(random() * 3)::int;
        FOR v_j IN 1..v_n_items LOOP
          IF v_product_id IS NULL AND v_prod_min IS NOT NULL THEN
            v_pick_id := floor(random() * (v_prod_max - v_prod_min + 1))::bigint + v_prod_min;
            SELECT id INTO v_product_id
            FROM public.products
            WHERE id >= v_pick_id
            ORDER BY id
            LIMIT 1;
          END IF;
          IF v_product_id IS NOT NULL THEN
            INSERT INTO public.order_items (
              id, order_id, product_id, quantity, unit_price
            ) VALUES (
              nextval('order_items_id_seq'::regclass),
              v_order_id,
              v_product_id,
              1 + floor(random() * 5)::int,
              round((random() * 100 + 1)::numeric, 2)
            )
            RETURNING id INTO v_item_id;
          END IF;
        END LOOP;

        UPDATE public.orders o
        SET total_amount = COALESCE((
          SELECT sum(quantity * unit_price) FROM public.order_items WHERE order_id = o.id
        ), 0)
        WHERE o.id = v_order_id;
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    -- UPDATE churn on existing rows
    BEGIN
      IF v_cust_min IS NOT NULL THEN
        v_pick_id := floor(random() * (v_cust_max - v_cust_min + 1))::bigint + v_cust_min;
        UPDATE public.customers
        SET is_active = NOT is_active
        WHERE id = (
          SELECT id FROM public.customers
          WHERE id >= v_pick_id
          ORDER BY id LIMIT 1
        );
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_prod_min IS NOT NULL THEN
        v_pick_id := floor(random() * (v_prod_max - v_prod_min + 1))::bigint + v_prod_min;
        UPDATE public.products
        SET stock = GREATEST(0, stock + (floor(random() * 11) - 5)::int),
            price = round((price * (0.95 + random() * 0.1))::numeric, 2)
        WHERE id = (
          SELECT id FROM public.products
          WHERE id >= v_pick_id
          ORDER BY id LIMIT 1
        );
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    BEGIN
      IF v_ord_min IS NOT NULL THEN
        v_pick_id := floor(random() * (v_ord_max - v_ord_min + 1))::bigint + v_ord_min;
        v_status := v_statuses[1 + floor(random() * array_length(v_statuses, 1))::int];
        UPDATE public.orders
        SET status = v_status
        WHERE id = (
          SELECT id FROM public.orders
          WHERE id >= v_pick_id
          ORDER BY id LIMIT 1
        );
      END IF;
    EXCEPTION WHEN others THEN
      NULL;
    END;

    IF ${WITH_DELETES_SQL} = 1 THEN
      v_delete_roll := random();
      IF v_delete_roll < 0.35 AND v_order_id IS NOT NULL THEN
        BEGIN
          DELETE FROM public.order_items WHERE order_id = v_order_id;
          DELETE FROM public.orders WHERE id = v_order_id;
          IF v_customer_id IS NOT NULL AND random() < 0.5 THEN
            DELETE FROM public.customers WHERE id = v_customer_id;
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
echo "Tip: CONNECT_URL=http://localhost:8083 POSTGRES_HOST=${DB_HOST} POSTGRES_PORT=${DB_PORT} \\"
echo "       ./scripts/cdc-status.sh --mode aks --instance ${INSTANCE}"

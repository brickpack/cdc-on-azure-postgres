# AKS deployment

Runs the CDC rollback pipeline on AKS for any number of database instances —
each its own Azure Postgres Flexible Server with its own original MySQL
rollback target, each optionally backed by its own Azure Key Vault. The
Docker Compose setup in the repo root is unchanged (single-VM path);
everything AKS lives in this folder.

Read the main [README](../README.md) first — architecture, rollback window,
type-transform validation, and rollback checklist apply unchanged. This file
is the **deploy + operate** runbook for AKS.

Infrastructure (RG, VNet, ACR, AKS, peering) is provisioned separately —
see [`../terraform/README.md`](../terraform/README.md).

## Order of operations

```text
A. Infra (terraform/)     apply + VNet peering / private DNS
B. Cluster access         kubeconfig, namespace, ACR pull/push secret
C. Config + secrets       values.local.yaml, Key Vault (check/create)
D. Connect image          build + push to ACR
E. Data path              Postgres CDC prep (after peering works)
F. Pipeline               Strimzi, helm install cdc-rollback, verify
G. Operate                status / load / compare / rollback / decommission
```

## Differences from Docker Compose

|                         | Docker Compose                                        | AKS (Helm)                                   |
| ----------------------- | ----------------------------------------------------- | -------------------------------------------- |
| Kafka                   | KRaft, single broker on one VM                        | Strimzi KRaft, 3 brokers RF=3                |
| Secrets                 | `instances/<name>.env` files on the VM                | Azure Key Vault CSI                          |
| Connectors deployed via | REST API (`scripts/`)                                 | `KafkaConnector` CRDs (`helm upgrade`)       |
| Scale                   | N instances, one shared single-broker Kafka on one VM | N instances, one shared 3-broker HA Kafka    |
| Rollback triggered via  | `scripts/deploy-rollback-sink.sh <name>`              | `aks/scripts/deploy-rollback-sink.sh <name>` |
| Rollback window (7 days)| `docker-compose.yml`                                  | `chart/templates/kafka.yaml`                 |

## Layout

```text
aks/
  chart/                         Helm: Kafka + Connect (Strimzi) + connectors
  values.example.yaml            copy → values.local.yaml (gitignored)
  scripts/deploy-rollback-sink.sh
  scripts/compare-mysql-pg-data.sh
  scripts/build-push-connect-image.sh
../terraform/                    Azure infra (apply first)
../scripts/                      cdc-status, monitor, simulate-shop-load, …
```

One chart, one release. Instances are entries in one values file; `helm
upgrade` is the deployment mechanism.

## Design decisions

- **One shared Kafka cluster (3 brokers, Strimzi, KRaft)** for every
  instance. RF=3 / `min.insync.replicas=2` — on AKS this cluster _is_ the
  rollback plan.
- **Per-instance topic prefix `cdc-<name>`** keeps rollbacks isolated.
- **Naming**: topics `cdc-<name>.*`, connectors `postgres-source-<name>` /
  `mysql-rollback-sink-<name>`, default slot/publication `cdc_<name>`
  (overridable via `postgres.slotName` / `postgres.publicationName`).
- **Passwords only in Key Vault.** Each `instances[]` entry has its own
  `keyVault:` block. Defaults: `cdc-<name>-postgres-password` /
  `cdc-<name>-mysql-password`. Override with `postgres.passwordSecret` /
  `mysql.passwordSecret` when the vault already uses other object names.
  Secret **values** must be the raw password string (not JSON).
- **`enabled` (default `true`)** gates that instance’s SecretProviderClass,
  Connect volume, and source connector so you can onboard one DB at a time.
  Flipping `enabled` restarts shared Connect briefly.
- **Connect image** from [`Dockerfile.connect`](Dockerfile.connect) (Strimzi
  base), not the Compose `../Dockerfile`.

---

## Install

Assumes you completed [`../terraform/README.md`](../terraform/README.md)
steps **1** (apply) and **7** (peering + DNS). Commands below are from
`from_work/cdc/` unless noted. Terraform outputs:

```bash
TF=../terraform   # if you are already in aks/; else from_work/cdc/terraform
eval "$(terraform -chdir="$TF" output -raw aks_get_credentials)"
```

### 1. kubeconfig + namespace

```bash
kubectl get nodes
kubectl create namespace cdc-rollback
```

### 2. ACR pull + optional push secret

```bash
# Cluster pull
az aks update \
  -g "$(terraform -chdir="$TF" output -raw resource_group_name)" \
  -n "$(terraform -chdir="$TF" output -raw aks_name)" \
  --attach-acr "$(terraform -chdir="$TF" output -raw acr_name)"

# Optional in-cluster push credentials (local build script uses az acr login)
kubectl create secret docker-registry acr-push-credentials \
  -n cdc-rollback \
  --docker-server="$(terraform -chdir="$TF" output -raw acr_login_server)" \
  --docker-username="$(terraform -chdir="$TF" output -raw acr_push_token_name)" \
  --docker-password="$(terraform -chdir="$TF" output -raw acr_push_token_password)" \
  --dry-run=client -o yaml | kubectl apply -f -
```

### 3. `aks/values.local.yaml`

```bash
cd aks   # or stay in cdc/ and use aks/ paths
cp values.example.yaml values.local.yaml
```

Fill from terraform outputs (`helm_values_snippet` is a cheat sheet — keep
image tag `3.9-cdc1`):

```yaml
connectImage: <acr_login_server>/cdc-kafka-connect:3.9-cdc1

instances:
  - name: toolbox                    # ^[a-z][a-z0-9]*$ — CDC nickname
    enabled: true                    # false until KV secrets exist
    keyVault:
      name: <key_vault_name>
      tenantId: <tenant_id>
      identityClientId: <keyvault_csi_client_id>
    postgres:
      host: <pg>.postgres.database.azure.com
      port: "5432"
      user: cdc_replication
      dbname: shop
      # passwordSecret: <existing-kv-object>   # if not cdc-toolbox-postgres-password
      # loadUser: pgadmin                      # for simulate-shop-load.sh only
    mysql:
      host: <mysql>.mysql.database.azure.com
      port: "3306"
      user: <rollback-user>
      dbname: shop
      # passwordSecret: <existing-kv-object>
```

### 4. Key Vault secrets

Passwords go **only** into Azure Key Vault — never into `values.local.yaml`.

**Default object names** (when creating): `cdc-<nickname>-postgres-password`
and `cdc-<nickname>-mysql-password` (nickname = `instances[].name`). Optional
load-test secret: `cdc-<nickname>-postgres-load-password`.

**Existing secrets:** do not copy passwords. Set `postgres.passwordSecret` /
`mysql.passwordSecret` to the vault object names:

```bash
VAULT="$(terraform -chdir="$TF" output -raw key_vault_name)"
az keyvault secret list --vault-name "$VAULT" --query "[].name" -o tsv
```

**Check first** — do not overwrite secrets that already exist:

```bash
INSTANCE=toolbox   # instances[].name

for n in \
  "cdc-${INSTANCE}-postgres-password" \
  "cdc-${INSTANCE}-mysql-password"
do
  # If you set passwordSecret in values, check those names instead
  if az keyvault secret show --vault-name "$VAULT" -n "$n" --query name -o tsv &>/dev/null; then
    echo "OK  exists: $n"
  else
    echo "MISSING: $n  ← create with set_kv_secret below"
  fi
done
```

**Create only if missing** (interactive; never `--value` on the CLI):

```bash
set_kv_secret() {
  local name="$1" prompt="$2"
  local tmp secret
  if az keyvault secret show --vault-name "$VAULT" -n "$name" --query name -o tsv &>/dev/null; then
    echo "skip $name (already exists)"
    return 0
  fi
  tmp="$(mktemp)"
  chmod 600 "$tmp"
  read -rs "secret?${prompt}: "
  echo
  secret="${secret//$'\r'/}"
  secret="${secret//$'\n'/}"
  printf '%s' "$secret" >"$tmp"
  unset secret
  az keyvault secret set --vault-name "$VAULT" -n "$name" \
    --file "$tmp" --encoding utf-8 >/dev/null
  rm -P "$tmp" 2>/dev/null || rm -f "$tmp"
  echo "set $name"
}

set_kv_secret "cdc-${INSTANCE}-postgres-password" \
  "Postgres password for user cdc_replication"
set_kv_secret "cdc-${INSTANCE}-mysql-password" \
  "MySQL password for rollback user"
```

Secret **values** must be the raw password string (not JSON / connection
URI). Verify no embedded newline with `-o json | od -c` (not `-o tsv`).

### 5. Build and push the Connect image

Requires Docker. From `aks/`:

```bash
./scripts/build-push-connect-image.sh
```

Uses `connectImage` from `values.local.yaml`. Re-run after any change to
`Dockerfile.connect`.

### 6. Confirm DB reachability from the cluster

Peering/DNS are configured in terraform README §7. Smoke test:

```bash
kubectl run -n cdc-rollback -it --rm netcheck --image=busybox:1.36 --restart=Never -- \
  sh -c 'nc -z -w 5 <pg-host>.postgres.database.azure.com 5432 && echo PG_OK; \
         nc -z -w 5 <mysql-host>.mysql.database.azure.com 3306 && echo MYSQL_OK'
```

### 7. Postgres CDC prep (each captured database)

Do the shared setup in the parent
[`README.md` Prerequisites](../README.md#prerequisites) / run
[`scripts/cdc_setup.sql`](../scripts/cdc_setup.sql). For this chart you need:

- Publication `cdc_<instances[].name>` (e.g. `cdc_toolbox`) with
  `FOR TABLES IN SCHEMA public, cdc` — not `FOR ALL TABLES`
- Table `cdc.debezium_heartbeat` (chart does not create it)
- Slot `cdc_<name>` created by Debezium on first start

If you used older `<DB_NAME>_cdc_*` names, set `postgres.slotName` /
`postgres.publicationName` in `values.local.yaml`.

### 8. Install Strimzi

Strimzi **0.45.0** matches Kafka **3.9.0**. On AKS Kubernetes 1.33+ set
`STRIMZI_KUBERNETES_VERSION` or the operator crash-loops:

```bash
helm install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 0.45.0 -n cdc-rollback

kubectl set env deployment/strimzi-cluster-operator -n cdc-rollback \
  STRIMZI_KUBERNETES_VERSION="major=1,minor=$(kubectl version -o json | sed -n 's/.*"minor": "\([0-9]*\).*/\1/p' | head -1)"

kubectl rollout status deployment/strimzi-cluster-operator -n cdc-rollback
```

### 9. Install the CDC Helm chart

```bash
# From cdc/ (parent of aks/)
helm install cdc-rollback aks/chart -n cdc-rollback -f aks/values.local.yaml

kubectl wait kafka/cdc-kafka --for=condition=Ready -n cdc-rollback --timeout=15m
kubectl wait kafkaconnect/cdc-connect --for=condition=Ready -n cdc-rollback --timeout=15m
kubectl get kafkaconnector -n cdc-rollback
```

If brokers stay `Pending` (`Insufficient cpu/memory`), chart requests do not
fit the node size — keep D2-sized defaults or raise CDC VM size + quota.

### 10. Verify

```bash
kubectl get kafkaconnector -n cdc-rollback   # enabled sources READY

# After DML on a published table:
kubectl exec -n cdc-rollback cdc-kafka-broker-0 -c kafka -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep cdc-

kubectl port-forward -n cdc-rollback svc/cdc-kafbat-ui 8080:8080
# http://localhost:8080
```

Chart uses `snapshot.mode: never` — only changes **after** the connector
starts are captured. Do not treat the rollback window as covering
pre-connector history.

---

## Normal operation

AKS status/monitor/load/compare scripts do **not** use `instances/<name>.env`.
They load host/user/db/slot from `aks/values.local.yaml` and passwords from
Key Vault (defaults or `passwordSecret` / `loadPasswordSecret`). Requires
`az login`.

Postgres and MySQL are private — **always** reach them through an SSH tunnel
on a jump host. Connect and Kafbat use `kubectl port-forward`.

```bash
# Terminal A — Postgres tunnel (use a free local port if 5432 is taken)
ssh -N -L 15432:<postgres-fqdn>:5432 -i ~/.ssh/<key> <user>@<jump-public-ip>
# or: az ssh vm -g <rg> -n <jump-vm> -- -N -L 15432:<postgres-fqdn>:5432

# Terminal B — MySQL tunnel (compare / verification)
ssh -N -L 13306:<mysql-fqdn>:3306 -i ~/.ssh/<key> <user>@<jump-public-ip>

# Terminal C — Connect REST API
kubectl port-forward -n cdc-rollback svc/cdc-connect-connect-api 8083:8083 &

CONNECT_URL=http://localhost:8083 \
POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
  ./scripts/cdc-status.sh --mode aks --instance toolbox

CONNECT_URL=http://localhost:8083 \
POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
  ./scripts/monitor-debezium.sh --mode aks --instance toolbox

POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
  ./scripts/simulate-shop-load.sh --instance toolbox --batches 20 --batch-size 50

POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=15432 \
MYSQL_HOST=127.0.0.1 MYSQL_PORT=13306 \
  ./aks/scripts/compare-mysql-pg-data.sh --instance toolbox \
    --table customers,orders,order_items,products --inspect 5
```

Optional: `CDC_VALUES=/path/to/values.local.yaml`.

Manual checks worth alerting on: `kubectl get kafkaconnector`, broker disk
(`df` on `/var/lib/kafka`), slot WAL lag via tunnel, MySQL `nc` from jump host.

```bash
kubectl port-forward -n cdc-rollback svc/cdc-kafbat-ui 8080:8080
```

## Rollback procedure (per instance)

The main README's checklist applies; only step 2 changes:

```bash
./aks/scripts/deploy-rollback-sink.sh <name>
```

No SSH tunnel and no credentials on the CLI: helm/kubectl only. MySQL target
from `values.local.yaml`; password from Key Vault in-cluster. Other instances
keep capturing.

Several at once: `--reuse-values` keeps only the latest `rollback:` list —
either clear completed sinks before the next, or set them together:

```bash
helm upgrade cdc-rollback aks/chart -n cdc-rollback --reuse-values --set 'rollback={billing,orders}'
```

### Pause / resume a running rollback

```bash
kubectl patch kafkaconnector mysql-rollback-sink-<name> -n cdc-rollback \
  --type merge -p '{"spec":{"state":"paused"}}'

kubectl get kafkaconnector mysql-rollback-sink-<name> -n cdc-rollback \
  -o jsonpath='{.status.connectorStatus.connector.state}{"\n"}'   # PAUSED

kubectl patch kafkaconnector mysql-rollback-sink-<name> -n cdc-rollback \
  --type merge -p '{"spec":{"state":"running"}}'
```

Do **not** use Connect REST `/pause` under Strimzi. Removing the sink is
`--set-json 'rollback=[]'` (below), not pause. A later `helm upgrade` that
re-renders the sink CR can clear `spec.state` — re-apply pause if needed.

## Post-rollback cleanup (per instance)

1. **Stop writing to MySQL** — clear the sink CR(s):

```bash
# Empty list — NOT --set rollback=null (with --reuse-values that keeps the old list)
helm upgrade cdc-rollback aks/chart -n cdc-rollback --reuse-values \
  --set-json 'rollback=[]'

kubectl get kafkaconnector -n cdc-rollback   # no mysql-rollback-sink-*
```

   Kafka topics and consumer groups remain (change log + ~7 day retention).
   Optional tidy for the idle sink group:

```bash
kubectl exec -n cdc-rollback cdc-kafka-broker-0 -c kafka -- \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --delete --group connect-mysql-rollback-sink-<name>
```

2. **Stop capture** (when done with CDC for that DB) — drop the slot via
   tunnel / jump host:

```sql
SELECT pg_drop_replication_slot('cdc_<name>');
```

## Decommission one instance (others keep running)

Shared Kafka/Connect stay up. Clean up only `<name>`:

| Shared (leave alone) | Per-instance |
|---|---|
| Strimzi / Kafka / Connect / namespace | `postgres-source-<name>`, KV mount |
| Other instances | Topics `cdc-<name>.*`, slot on **that** Postgres |

1. If a sink is deployed for this instance only, remove it from `rollback`
   without clearing other active rollbacks
   (`--set-json 'rollback=["billing"]'` etc.).
2. In `aks/values.local.yaml` set that instance’s `enabled: false`, then:

```bash
helm upgrade cdc-rollback aks/chart -n cdc-rollback -f aks/values.local.yaml
```

   Use `-f` (not only `--reuse-values`) so Helm reads the edited flag.
   Connect restarts briefly.
3. Drop that instance’s Postgres slot (tunnel).
4. Optional: delete `cdc-<name>.*` topics and consumer groups for that prefix.
5. Optional: remove KV secrets / the `instances[]` block when you will not
   re-enable it.

Only when **no** instances remain → Full teardown, then terraform destroy.

## Full teardown

Destroys the shared Kafka log for everyone. Then destroy Azure infra
([terraform README](../terraform/README.md#destroy)).

```bash
helm uninstall cdc-rollback -n cdc-rollback || true
helm uninstall strimzi -n cdc-rollback || true
kubectl delete pvc -n cdc-rollback -l strimzi.io/cluster=cdc-kafka --wait=false
kubectl delete pod -n cdc-rollback --all --force --grace-period=0 --wait=false
```

Drop remaining replication slots via the Postgres tunnel / jump host. Then
`terraform destroy` in `terraform/`.

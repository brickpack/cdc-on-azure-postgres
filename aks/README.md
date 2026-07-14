# AKS deployment

Runs the CDC rollback pipeline on an existing AKS cluster for **five
database instances** (five separate Azure Postgres Flexible Servers, each
with its own original MySQL rollback target). The Docker Compose setup in
the repo root is unchanged and remains the single-VM path; everything AKS
lives in this folder.

Read the main [README](../README.md) first -- the architecture, rollback
window, type-transform validation, and rollback checklist all apply
unchanged. This file only covers what is different on AKS.

## Layout

```text
aks/
  chart/                      Helm chart: Kafka + Kafka Connect (Strimzi) + connectors
  values.example.yaml         copy to aks/values.local.yaml (gitignored) and fill in
  scripts/deploy-rollback-sink.sh   emergency rollback, per instance
```

One chart, one release. The five instances are entries in one values file;
`helm upgrade` is the only deployment mechanism.

## Design decisions

- **One shared Kafka cluster (3 brokers, Strimzi, KRaft -- no ZooKeeper)**
  for all five instances, not five stacks. Everything the Compose file runs
  at replication factor 1 runs at RF=3 / `min.insync.replicas=2` here,
  because on AKS this cluster _is_ the rollback plan.
- **Per-instance topic prefix `cdc-<name>`** (the Docker setup uses plain
  `cdc`). This is what makes rollback per-instance safe: each sink's
  `topics.regex` matches only its own instance, so rolling back one database
  can never replay another database's changes.
- **Fixed naming convention** derived from the instance name: topics
  `cdc-<name>.*`, replication slot and publication `cdc_<name>`, connectors
  `postgres-source-<name>` / `mysql-rollback-sink-<name>`. No per-instance
  knobs beyond connection details and the table list.
- **Connectors are `KafkaConnector` resources** managed by Strimzi, so the
  Docker REST-API deploy scripts have no AKS equivalent -- `helm upgrade` is
  the deploy. The `schema.history.internal.*` settings from
  `connectors/postgres-source.json` are dropped: the Postgres connector
  doesn't use them (they matter for the MySQL/SQL Server connectors).
- **All passwords live in Azure Key Vault, nowhere else.** The Key Vault
  CSI add-on mounts them into the Connect pods as tmpfs files, and Kafka's
  built-in `DirectoryConfigProvider` resolves them when a connector starts.
  Connector configs -- in the chart, in the Helm release, and in the Connect
  config topic -- contain only a `${directory:/mnt/keyvault:...}`
  placeholder. `values.local.yaml` holds no secrets (it stays gitignored
  anyway). Naming convention in the vault: `cdc-<name>-postgres-password`
  and `cdc-<name>-mysql-password`, and all ten must exist before install --
  the mount fails otherwise, which doubles as day-one proof the rollback
  credentials are really in the vault. Rotating a password: update it in
  Key Vault, then restart Connect
  (`kubectl rollout restart -n cdc-rollback deployment/cdc-connect-connect`).
- **Rollback targets are declared up front.** Each instance's `mysql:` block
  in `values.local.yaml` names its original source MySQL (no password), so
  triggering a rollback takes nothing but the instance name.
- **Connect image**: built from the project [`Dockerfile`](../Dockerfile) and
  pushed to ACR before first deploy (and after any Dockerfile change) using
  [`scripts/build-push-connect-image.sh`](scripts/build-push-connect-image.sh).
  The same image is used locally by Docker Compose for testing.

## Prerequisites

0. **AKS cluster** -- this folder assumes one already exists. To create a
   dedicated, cost-minimized stack (RG, VNet, ACR, Key Vault, AKS + CDC
   node pool) with no dependency on other Azure resources, use
   [`../terraform/`](../terraform/).

1. **Strimzi operator** installed and watching your target namespace, a
   0.45.x release (supports Kafka 3.9.0, which the chart pins -- newer
   Strimzi lines require Kafka 4.x; validate the Debezium 2.4.2 plugin
   before moving to those):

   ```bash
   kubectl create namespace cdc-rollback
   helm install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator \
     --version 0.45.0 -n cdc-rollback
   ```

2. **ACR**: the cluster must be able to pull from your ACR:

   ```bash
   az aks update -g <rg> -n <cluster> --attach-acr <acr>
   ```

   Then build and push the Connect image before deploying:

   ```bash
   ./aks/scripts/build-push-connect-image.sh
   ```

   Set `CONNECT_IMAGE` in your environment (or edit the script) if you want
   a tag other than the default. Re-run whenever the Dockerfile changes.

3. **Key Vault**: the Key Vault CSI add-on enabled
   (`az aks enable-addons --addons azure-keyvault-secrets-provider -g <rg> -n <cluster>`),
   its identity granted secret-get on the vault (assumed already in place),
   and one secret per password under the naming convention -- for each of
   the five instances:

   ```bash
   az keyvault secret set --vault-name <vault> -n cdc-<name>-postgres-password --value '<...>'
   az keyvault secret set --vault-name <vault> -n cdc-<name>-mysql-password --value '<...>'
   ```

   Fill the `keyVault:` block in `values.local.yaml` with the vault name,
   tenant id, and the add-on identity's clientId (query shown in
   [values.example.yaml](values.example.yaml)).

4. **Network reachability -- verify now, not during an incident.** The AKS
   VNet must reach all five Postgres servers (port 5432) **and all five
   original MySQL servers (port 3306)** via private endpoints/VNet peering
   with working private DNS. The MySQL path is the one that rots unnoticed,
   since nothing uses it day-to-day. Check each one from inside the cluster:

   ```bash
   kubectl run -n cdc-rollback -it --rm netcheck --image=busybox:1.36 --restart=Never -- \
     nc -z -w 5 <mysql-host> 3306 && echo REACHABLE
   ```

   Re-run these checks periodically (or wire them into your existing
   alerting) for as long as the rollback window matters.

5. **Per-server Postgres prep** -- same as the main README prerequisites
   (`wal_level=logical`, replication user), on **each** of the five servers,
   with the publication named by convention:

   ```sql
   CREATE PUBLICATION cdc_<name> FOR ALL TABLES;
   ```

6. **Capacity**: 3 brokers with 4Gi each plus 2 Connect pods with 2Gi each.
   If the cluster runs latency-sensitive app workloads, consider a dedicated
   node pool so nothing evicts the brokers holding the rollback log.

## Install (pre-cutover, per the main README timeline)

```bash
cp aks/values.example.yaml aks/values.local.yaml   # fill in all five instances
helm install cdc-rollback aks/chart -n cdc-rollback -f aks/values.local.yaml
kubectl wait kafka/cdc-kafka --for=condition=Ready -n cdc-rollback --timeout=15m
kubectl wait kafkaconnect/cdc-connect --for=condition=Ready -n cdc-rollback --timeout=30m  # first run builds the image
kubectl get kafkaconnector -n cdc-rollback   # all sources READY
```

Then, exactly as in the main README: **do not cut over an instance until its
initial snapshot is complete** and the connector is RUNNING. Check per
instance:

```bash
kubectl logs -n cdc-rollback -l strimzi.io/cluster=cdc-connect --tail=-1 \
  | grep "Snapshot ended"
```

and verify a test write reaches its topic:

```bash
kubectl exec -n cdc-rollback cdc-kafka-broker-0 -c kafka -- \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic cdc-<name>.<schema>.<table> --from-beginning --max-messages 1
```

## Normal operation

`scripts/monitor-debezium.sh` is Docker-specific; the AKS equivalents:

- **Connector state** (the critical one -- every minute a source is down is
  a gap in that instance's change log):
  `kubectl get kafkaconnector -n cdc-rollback` -- all five sources READY.
- **Broker disk** (a full disk silently ends the rollback window):
  `kubectl exec -n cdc-rollback cdc-kafka-broker-0 -c kafka -- df -h /var/lib/kafka`
- **Replication slot WAL lag** -- same SQL as the main README, against each
  of the five servers (slot name `cdc_<name>`).
- **MySQL reachability** -- the `nc` check from Prerequisites, per server.

Wire whichever of these you can into your cluster's existing
Prometheus/alerting (Strimzi exposes metrics if you want to go further);
at minimum, alert on connector readiness and broker disk.

## Rollback procedure (per instance)

The main README's checklist applies; only step 2 changes:

```bash
./aks/scripts/deploy-rollback-sink.sh <name>
```

No credentials on the command line: the MySQL target comes from that
instance's `mysql:` block in `values.local.yaml`, the password from Key
Vault. The script shows the target, confirms interactively, deploys the
sink for that one instance via `helm upgrade`, and watches connector state
and consumer lag until the replay catches up, then prints the remaining
verification steps. The other four instances keep capturing changes,
unaffected.

Rolling back several instances at once? `--reuse-values` keeps only the
_latest_ `rollback:` list, so either remove each completed sink
(`--set rollback=null`) before running the script for the next instance, or
deploy them together in one upgrade:

```bash
helm upgrade cdc-rollback aks/chart -n cdc-rollback --reuse-values --set 'rollback={billing,orders}'
```

## Post-rollback cleanup (per instance)

```bash
helm upgrade cdc-rollback aks/chart -n cdc-rollback --reuse-values --set rollback=null
```

Then drop that instance's slot on its Postgres server:

```sql
SELECT pg_drop_replication_slot('cdc_<name>');
```

## Full teardown

```bash
helm uninstall cdc-rollback -n cdc-rollback
kubectl delete pvc -n cdc-rollback -l strimzi.io/cluster=cdc-kafka  # disks survive uninstall by design
```

Then drop the `cdc_<name>` replication slot on every Postgres server.

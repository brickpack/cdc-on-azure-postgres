# AKS Terraform (standalone)

Provisions a **greenfield** Azure stack for the CDC rollback pipeline in
[`aks/`](../aks/). It has no dependency on any existing Azure resources or on
the Docker Compose path -- apply this, then follow [`aks/README.md`](../aks/README.md)
to install Strimzi + the Helm chart.

## What it creates

| Resource | Notes |
| --- | --- |
| Resource group | All of the below live here |
| VNet + AKS subnet | Azure CNI Overlay (small subnet, cheap) |
| Optional VNet peerings | Local half only -- see `peer_vnet_ids` |
| ACR (Basic) | Holds the Strimzi-built Connect image; push token for the chart |
| AKS (Free tier) | Key Vault CSI add-on; API locked to your CIDRs |
| System node pool | 1× `Standard_D2ds_v4` (kube-system, Strimzi operator) |
| CDC node pool | 3× `Standard_D2ds_v4`, labeled `workload=cdc-rollback` |
| Role assignment | `Key Vault Secrets User` on an **existing** vault for the CSI identity |

Does **not** create a Key Vault. Point `key_vault_name` /
`key_vault_resource_group_name` at the vault that already holds the DB
passwords (e.g. `dbmig-dev-kv` in `dbmig-dev-rg`). Secret names must still
match the chart convention: `cdc-<name>-postgres-password` /
`cdc-<name>-mysql-password`.

Sized for Kafka RF=3 (three CDC nodes). Prefer `Standard_D4ds_v4` for the
CDC pool once family quota allows (>= 14 vCPU).

## Cost floor (intentional)

- AKS **Free** tier (no Uptime SLA)
- **No** Azure Monitor / OMS agent
- ACR **Basic**
- `*_ds_v4` VMs + **ephemeral** OS disks (fits common ~10 vCPU/family quotas)
- Public API server (restricted by IP) instead of a private cluster + jump box
- Reuses an existing Key Vault (no second vault to pay for / manage)

Rough order of magnitude (West US 2, pay-as-you-go, compute only): ~4 VMs.
Kafka broker disks (`256Gi` premium CSI, from the Helm chart -- not this
module) dominate storage cost once the chart is installed.

## Prerequisites

1. Azure CLI logged in with rights to create RGs, AKS, ACR, networking, and
   to assign `Key Vault Secrets User` on the existing vault.
2. Terraform >= 1.5.
3. Your public IP (and any CI runner CIDRs) for the AKS API allowlist.
4. An existing Key Vault with the DB passwords (or ready to receive them
   under the chart naming convention).

```bash
curl -s https://ifconfig.me
```

## Apply

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# fill in subscription_id and api_server_authorized_ip_ranges
terraform init
terraform plan
terraform apply
```

Useful outputs after apply:

```bash
terraform output aks_get_credentials
terraform output -raw helm_values_snippet
terraform output -raw acr_push_token_password   # sensitive
```

## Wire up for the Helm chart

After `terraform apply`, still outside this module:

1. **kubeconfig**
   ```bash
   eval "$(terraform output -raw aks_get_credentials)"
   ```

2. **ACR push secret** (Strimzi Connect image build) -- same as
   [`aks/README.md`](../aks/README.md) prerequisites:
   ```bash
   kubectl create namespace cdc-rollback
   kubectl create secret docker-registry acr-push-credentials \
     -n cdc-rollback \
     --docker-server="$(terraform output -raw acr_login_server)" \
     --docker-username="$(terraform output -raw acr_push_token_name)" \
     --docker-password="$(terraform output -raw acr_push_token_password)"
   ```

3. **`aks/values.local.yaml`** -- paste `helm_values_snippet`, then add the
   five `instances:` blocks from [`aks/values.example.yaml`](../aks/values.example.yaml).

4. **Key Vault secrets** in the **existing** vault named by
   `key_vault_name` (naming convention required by the chart):
   ```bash
   az keyvault secret set --vault-name "$(terraform output -raw key_vault_name)" \
     -n cdc-<name>-postgres-password --value '...'
   az keyvault secret set --vault-name "$(terraform output -raw key_vault_name)" \
     -n cdc-<name>-mysql-password --value '...'
   ```
   Skip seeding if those secrets already exist under those exact names.

5. **Database reachability** -- peer this VNet (`terraform output -raw vnet_id`)
   to the VNets hosting the five Postgres + MySQL Flexible Servers, and ensure
   private DNS resolves. Set `peer_vnet_ids` to create the local peering half
   from this module; create the remote half (and DNS) on the DB side. Then run
   the `nc` checks in [`aks/README.md`](../aks/README.md).

6. Install Strimzi + the chart per [`aks/README.md`](../aks/README.md).

## Network notes

- Overlay pod CIDR is `10.244.0.0/16` (cluster-internal only). Nodes use
  `aks_subnet_prefix` (default `10.240.0.0/22`). Change `vnet_address_space`
  if it collides with a DB VNet you will peer.
- This module does **not** create private endpoints or private DNS for
  Postgres/MySQL -- those stay with the database owners.

## Destroy

```bash
# Helm/Strimzi first if installed -- broker PVCs are retained by design
helm uninstall cdc-rollback -n cdc-rollback || true
kubectl delete pvc -n cdc-rollback -l strimzi.io/cluster=cdc-kafka || true

terraform destroy
```

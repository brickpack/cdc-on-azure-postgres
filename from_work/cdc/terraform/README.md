# AKS Terraform (infra only)

Provisions the Azure foundation for the CDC rollback pipeline in
[`aks/`](../aks/): resource group, VNet, ACR, AKS (with Key Vault CSI), and
the CSI identity’s `Key Vault Secrets User` role on an **existing** vault.

**This file stops at infrastructure.** After apply (and peering), continue
the deploy runbook in [`aks/README.md`](../aks/README.md) — kubeconfig,
values, Key Vault secrets, Connect image, Postgres prep, Strimzi, Helm,
verify, and day-2 ops.

```text
A. HERE     terraform apply + VNet peering / private DNS
B. aks/     everything else (see aks/README.md “Install”)
```

Teardown: uninstall Helm / delete broker PVCs **before** `terraform destroy`
(see Destroy below and aks README full teardown).

## What it creates

| Resource | Notes |
| --- | --- |
| Resource group | All of the below live here |
| VNet + AKS subnet | Azure CNI Overlay (small subnet, cheap) |
| Optional VNet peerings | Local half only — see `peer_vnet_ids` |
| ACR (Standard) | Holds the Connect image; scoped push token |
| AKS (Free tier) | Key Vault CSI add-on; API locked to your CIDRs |
| System node pool | 1× `Standard_D2ds_v4` (CriticalAddonsOnly) |
| CDC node pool | 3× `Standard_D2ds_v4`, labeled `workload=cdc-rollback` |
| Role assignment | `Key Vault Secrets User` on an **existing** vault for the CSI identity |

Does **not** create a Key Vault. Point `key_vault_name` /
`key_vault_resource_group_name` at the vault that holds (or will hold) DB
passwords. How secret names are chosen and how to create missing ones is
documented in [`aks/README.md`](../aks/README.md) (Key Vault section).

The Helm chart is sized for **3× D2ds_v4** (broker `500m`/`2500Mi`, Connect
1 replica). Prefer `Standard_D4ds_v4` for the CDC pool once **Total Regional
vCPUs** and **Standard DDSv4 Family vCPUs** in the region are ≥ ~20.

```bash
az vm list-usage -l westus2 -o table | grep -iE 'Ddsv4|Total Regional vCPUs'
```

## Cost floor (intentional)

- AKS **Free** tier (no Uptime SLA)
- **No** Azure Monitor / OMS agent
- ACR **Standard**
- `*_ds_v4` VMs + **ephemeral** OS disks
- Public API server (CIDR-restricted), not a private cluster + jump box
- Reuses an existing Key Vault

Kafka broker disks (`256Gi` premium CSI from the Helm chart) dominate storage
cost once the chart is installed.

## Prerequisites

1. Azure CLI logged in with rights to create RGs, AKS, ACR, networking, and
   to assign `Key Vault Secrets User` on the existing vault.
2. Terraform >= 1.5.
3. Your public IP (and any CI runner CIDRs) for the AKS API allowlist.
4. An existing Key Vault.
5. Postgres Flexible Server + MySQL already on a VNet you can peer
   (private access; public network disabled is fine).

```bash
curl -4 -s https://ifconfig.me
az account show --query id -o tsv
```

---

## 1. Terraform apply

```bash
cd from_work/cdc/terraform   # or aks/terraform if that is your tree
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars:
#   subscription_id
#   api_server_authorized_ip_ranges = ["YOUR.PUBLIC.IP/32"]
#   key_vault_name / key_vault_resource_group_name
#   peer_vnet_ids = [".../virtualNetworks/<db-vnet>"]   # recommended now
terraform init
terraform plan
terraform apply
```

Useful outputs:

```bash
terraform output -raw aks_get_credentials
terraform output -raw acr_login_server
terraform output -raw keyvault_csi_client_id
terraform output -raw vnet_id
terraform output -raw helm_values_snippet
```

`helm_values_snippet` is a **cheat sheet** for filling placeholders in
`aks/values.local.yaml` (image tag must stay `4.3-cdc1`). Full values and
chart install steps are in [`aks/README.md`](../aks/README.md).

---

## 2. VNet peering + private DNS

Postgres/MySQL Flexible Servers with private access resolve via private DNS
zones linked only to the DB VNet. AKS needs:

1. **Bidirectional peering** (no overlapping CIDRs — this module defaults to
   `10.240.0.0/16`; many DB VNets use `10.0.0.0/16`).
2. **Private DNS zone links** from each DB private zone onto the AKS VNet.

### 2a. Local half (Terraform)

Set in `terraform.tfvars` and apply (can be part of step 1):

```hcl
peer_vnet_ids = [
  "/subscriptions/<sub>/resourceGroups/<db-rg>/providers/Microsoft.Network/virtualNetworks/<db-vnet>",
]
```

### 2b. Remote half + DNS links (Azure CLI on the DB side)

```bash
AKS_VNET="$(cd ../terraform && terraform output -raw vnet_id)"
DB_RG=dbmig-dev-rg          # your DB resource group
DB_VNET=dbmig-dev-vnet      # your DB VNet name

# Remote peering (DB → AKS)
az network vnet peering create \
  -g "$DB_RG" --vnet-name "$DB_VNET" -n to-cdc-aks \
  --remote-vnet "$AKS_VNET" \
  --allow-vnet-access --allow-forwarded-traffic

# Link each Flexible Server private DNS zone to the AKS VNet
# (zone names are whatever Azure created for your servers)
az network private-dns link vnet create \
  -g "$DB_RG" \
  -z <pg-server>.private.postgres.database.azure.com \
  -n cdc-aks-pg-dns-link \
  -v "$AKS_VNET" -e false

az network private-dns link vnet create \
  -g "$DB_RG" \
  -z <mysql-server>.private.mysql.database.azure.com \
  -n cdc-aks-mysql-dns-link \
  -v "$AKS_VNET" -e false
```

After peering, verify reachability from the cluster (namespace must exist —
created in the aks README install steps):

```bash
kubectl run -n cdc-rollback -it --rm netcheck --image=busybox:1.36 --restart=Never -- \
  sh -c 'nslookup <pg-host>.postgres.database.azure.com; \
         nc -z -w 5 <pg-host>.postgres.database.azure.com 5432 && echo PG_OK; \
         nc -z -w 5 <mysql-host>.mysql.database.azure.com 3306 && echo MYSQL_OK'
```

Both must print `PG_OK` / `MYSQL_OK` before Connect can reach the databases.

## Network notes

- Overlay pod CIDR is `10.244.0.0/16` (cluster-internal only). Nodes use
  `aks_subnet_prefix` (default `10.240.0.0/22`).
- This module does **not** create private endpoints or private DNS for
  Postgres/MySQL — only the local peering half when `peer_vnet_ids` is set.
- Changing CDC `vm_size` requires `temporary_name_for_rotation` (already set
  in `aks.tf`) and spare regional vCPU for a temporary pool during rotation.

---

## Destroy

**Always uninstall Helm and delete broker PVCs before `terraform destroy`.**
Otherwise Azure Disks stay attached, the CDC node pool sticks in `Deleting`,
and Terraform times out (~60m). App teardown commands live in
[`aks/README.md`](../aks/README.md) (Full teardown); then:

```bash
eval "$(cd from_work/cdc/terraform && terraform output -raw aks_get_credentials)"

# From aks README: helm uninstall cdc-rollback + strimzi, delete PVCs/pods

cd from_work/cdc/terraform
terraform destroy
```

If destroy fails because state still has a pool Azure already deleted:

```bash
terraform state rm azurerm_kubernetes_cluster_node_pool.cdc
terraform destroy
```

Optional cleanup on the DB side (not managed by this module): remote peering
`to-cdc-aks`, private DNS links `cdc-aks-*-dns-link`, and Postgres
replication slots/publications when you are done capturing.

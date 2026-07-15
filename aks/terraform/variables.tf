variable "subscription_id" {
  description = "Azure subscription ID (required by azurerm 4.x for plan/apply)."
  type        = string
}

variable "name_prefix" {
  description = "Short prefix for all resource names (lowercase alphanumeric, start with a letter)."
  type        = string
  default     = "cdc"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,7}$", var.name_prefix))
    error_message = "name_prefix must match ^[a-z][a-z0-9]{1,7}$."
  }
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "westus2"
}

variable "tags" {
  description = "Tags applied to every resource."
  type        = map(string)
  default = {
    project = "cdc-on-azure-postgres"
    purpose = "rollback-window"
  }
}

# --- Network -----------------------------------------------------------------

variable "vnet_address_space" {
  description = "Address space for the CDC VNet. Must not overlap peered DB VNets."
  type        = list(string)
  default     = ["10.240.0.0/16"]
}

variable "aks_subnet_prefix" {
  description = "Subnet CIDR for AKS nodes (Azure CNI Overlay -- pods use a separate overlay CIDR)."
  type        = string
  default     = "10.240.0.0/22"
}

variable "peer_vnet_ids" {
  description = <<-EOT
    Optional remote VNet resource IDs to peer with so the cluster can reach
    Postgres (5432) and MySQL (3306) over private IPs. Both sides of each
    peering must exist; this module only creates the local half. Leave empty
    and peer manually if the remote side is owned elsewhere.
  EOT
  type        = list(string)
  default     = []
}

# --- Existing Key Vault (Postgres/MySQL secrets live here) -------------------

variable "key_vault_name" {
  description = <<-EOT
    Name of an existing Key Vault that holds the DB passwords. This module
    does not create a vault -- it grants the AKS CSI identity
    "Key Vault Secrets User" on it. Chart secret names must match
    cdc-<name>-postgres-password / cdc-<name>-mysql-password.
  EOT
  type        = string
}

variable "key_vault_resource_group_name" {
  description = "Resource group of the existing Key Vault (often the DB migration RG)."
  type        = string
}

# --- AKS API access ----------------------------------------------------------

variable "api_server_authorized_ip_ranges" {
  description = <<-EOT
    CIDRs allowed to reach the AKS public API server. Required -- an open
    API server is not acceptable even on Free tier. Include your current
    public IP /32 and any CI runner ranges.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.api_server_authorized_ip_ranges) > 0
    error_message = "api_server_authorized_ip_ranges must contain at least one CIDR."
  }
}

# --- Node pools (cost floor for the existing Helm chart) ---------------------

variable "system_node_vm_size" {
  description = <<-EOT
    VM size for the 1-node system pool (coreDNS, CSI, Strimzi operator).
    Must have a local temp disk if os_disk_type=Ephemeral. Default avoids
    DADSv5/DSv5 (often quota=0 on new subscriptions).
  EOT
  type        = string
  default     = "Standard_D2ds_v4"
}

variable "cdc_node_vm_size" {
  description = <<-EOT
    VM size for the 3-node CDC pool. Preferred is D4-class (16Gi) so one
    Kafka broker (4Gi) + Connect (2Gi) fit after kube-reserved. Default is
    D2ds_v4 (8Gi / 2 vCPU) because many subscriptions only have ~10 vCPU
    on DDSv4 -- 3xD4 + system exceeds that. Raise to Standard_D4ds_v4
    (and request quota >= 14 on that family) when you can; until then the
    chart's 4Gi broker requests may need a temporary trim.
  EOT
  type        = string
  default     = "Standard_D2ds_v4"
}

variable "cdc_node_count" {
  description = "CDC node count. Must be >= 3 so Kafka RF=3 can place one broker per node."
  type        = number
  default     = 3

  validation {
    condition     = var.cdc_node_count >= 3
    error_message = "cdc_node_count must be >= 3 (Kafka default.replication.factor=3)."
  }
}

variable "kubernetes_version" {
  description = "AKS Kubernetes version. Empty = Azure's current default for the region."
  type        = string
  default     = null
}

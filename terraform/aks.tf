resource "azurerm_kubernetes_cluster" "cdc" {
  name                = local.aks_name
  location            = azurerm_resource_group.cdc.location
  resource_group_name = azurerm_resource_group.cdc.name
  dns_prefix          = local.aks_name
  kubernetes_version  = var.kubernetes_version
  sku_tier            = "Free"

  # Cost: no Azure Monitor / OMS agent. Use kubectl + Strimzi metrics if needed.
  # Cost: no Azure Policy addon.
  # Cost: Azure CNI Overlay (small subnet, no per-pod VNet IPs).
  network_profile {
    network_plugin      = "azure"
    network_plugin_mode = "overlay"
    network_policy      = "azure"
    pod_cidr            = local.pod_cidr
    service_cidr        = local.service_cidr
    dns_service_ip      = local.dns_service_ip
    load_balancer_sku   = "standard"
    outbound_type       = "loadBalancer"
  }

  # Public API, locked to operator/CI CIDRs. Private cluster would need a
  # jump host and costs more to operate for this workload.
  api_server_access_profile {
    authorized_ip_ranges = var.api_server_authorized_ip_ranges
  }

  identity {
    type = "SystemAssigned"
  }

  # Required by aks/chart templates/keyvault.yaml (SecretProviderClass).
  key_vault_secrets_provider {
    secret_rotation_enabled  = true
    secret_rotation_interval = "2m"
  }

  # Tiny system pool: kube-system + Strimzi operator. Not CriticalAddonsOnly
  # -- the existing aks/chart has no CriticalAddonsOnly toleration, and
  # Strimzi must be able to schedule here.
  default_node_pool {
    host_encryption_enabled      = true
    name                         = "system"
    vm_size                      = var.system_node_vm_size
    node_count                   = 1
    vnet_subnet_id               = azurerm_subnet.aks.id
    os_disk_type                 = "Ephemeral"
    os_disk_size_gb              = 50
    zones                        = ["1", "2", "3"]
    only_critical_addons_enabled = true

    upgrade_settings {
      max_surge = "10%"
    }
  }

  tags = var.tags

  lifecycle {
    ignore_changes = [
      # Azure may bump the patch version; don't fight it on every plan.
      kubernetes_version,
    ]
  }
}

# Dedicated CDC pool: 3 nodes so Kafka RF=3 can place one broker per node.
# Labeled but not tainted -- aks/chart has no matching toleration yet; add a
# taint + chart tolerations later if you need hard isolation from other pods.
resource "azurerm_kubernetes_cluster_node_pool" "cdc" {
  name                  = "cdc"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.cdc.id
  vm_size               = var.cdc_node_vm_size
  node_count            = var.cdc_node_count
  vnet_subnet_id        = azurerm_subnet.aks.id
  mode                  = "User"
  # D2ds_v4 temp disk is 75 GiB -- ephemeral OS must fit under that.
  os_disk_type    = "Ephemeral"
  os_disk_size_gb = 64
  zones           = ["1", "2", "3"]

  node_labels = {
    "workload" = "cdc-rollback"
  }

  upgrade_settings {
    max_surge = "1"
  }

  tags = var.tags
}

# AKS kubelet pulls from ACR (Connect runtime image + any base layers).
resource "azurerm_role_assignment" "aks_acr_pull" {
  scope                = azurerm_container_registry.cdc.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_kubernetes_cluster.cdc.kubelet_identity[0].object_id
}

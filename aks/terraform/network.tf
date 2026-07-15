resource "azurerm_resource_group" "cdc" {
  name     = local.rg_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_virtual_network" "cdc" {
  name                = local.vnet_name
  location            = azurerm_resource_group.cdc.location
  resource_group_name = azurerm_resource_group.cdc.name
  address_space       = var.vnet_address_space
  tags                = var.tags
}

resource "azurerm_subnet" "aks" {
  name                 = "aks"
  resource_group_name  = azurerm_resource_group.cdc.name
  virtual_network_name = azurerm_virtual_network.cdc.name
  address_prefixes     = [var.aks_subnet_prefix]

  # Optional: if the existing Key Vault later gets network ACLs, add this
  # subnet to that vault's virtual_network_subnet_ids (outside this module).
  service_endpoints = ["Microsoft.KeyVault"]
}

# Local half of each peering. The remote VNet owner must create the matching
# remote half (and private DNS for *.postgres.database.azure.com /
# *.mysql.database.azure.com) before the cluster can reach the databases.
# PeeringState stays Initiated until the remote half exists -- that is
# expected, not a failure.
resource "azurerm_virtual_network_peering" "to_remote" {
  for_each = { for id in var.peer_vnet_ids : id => id }

  name                      = "to-${substr(md5(each.value), 0, 8)}"
  resource_group_name       = azurerm_resource_group.cdc.name
  virtual_network_name      = azurerm_virtual_network.cdc.name
  remote_virtual_network_id = each.value
  allow_forwarded_traffic   = true
  allow_gateway_transit     = false
  use_remote_gateways       = false
}

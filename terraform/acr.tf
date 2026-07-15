# Basic SKU: cheapest registry that still supports the Strimzi Connect image
# build (push from the cluster) and AKS pull via kubelet identity.
resource "azurerm_container_registry" "cdc" {
  name                          = local.acr_name
  resource_group_name           = azurerm_resource_group.cdc.name
  location                      = azurerm_resource_group.cdc.location
  sku                           = "Standard"
  admin_enabled                 = false
  quarantine_policy_enabled     = true
  public_network_access_enabled = false
  tags                          = var.tags
}

# Token used by Strimzi's KafkaConnect build to push the Connect image.
# Scoped to push+pull on repositories under this registry.
resource "azurerm_container_registry_scope_map" "connect_push" {
  name                    = "cdc-connect-push"
  container_registry_name = azurerm_container_registry.cdc.name
  resource_group_name     = azurerm_resource_group.cdc.name
  actions = [
    "repositories/*/content/read",
    "repositories/*/content/write",
  ]
}

resource "azurerm_container_registry_token" "connect_push" {
  name                    = "cdc-connect-push"
  container_registry_name = azurerm_container_registry.cdc.name
  resource_group_name     = azurerm_resource_group.cdc.name
  scope_map_id            = azurerm_container_registry_scope_map.connect_push.id
}

resource "azurerm_container_registry_token_password" "connect_push" {
  container_registry_token_id = azurerm_container_registry_token.connect_push.id

  password1 {}
}

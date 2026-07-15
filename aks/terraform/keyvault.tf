# Existing Key Vault that already holds Postgres/MySQL passwords.
# This module does NOT create a vault -- it only grants the AKS Key Vault
# CSI identity read access so Connect pods can mount secrets.
data "azurerm_key_vault" "cdc" {
  name                = var.key_vault_name
  resource_group_name = var.key_vault_resource_group_name
}

# CSI add-on identity can read secrets into Connect pods.
resource "azurerm_role_assignment" "kv_secrets_user" {
  scope                = data.azurerm_key_vault.cdc.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_kubernetes_cluster.cdc.key_vault_secrets_provider[0].secret_identity[0].object_id
}

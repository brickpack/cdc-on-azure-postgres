output "resource_group_name" {
  description = "Resource group holding the CDC AKS stack."
  value       = azurerm_resource_group.cdc.name
}

output "aks_name" {
  description = "AKS cluster name."
  value       = azurerm_kubernetes_cluster.cdc.name
}

output "aks_get_credentials" {
  description = "Fetch kubeconfig for the cluster."
  value       = "az aks get-credentials -g ${azurerm_resource_group.cdc.name} -n ${azurerm_kubernetes_cluster.cdc.name} --overwrite-existing"
}

output "vnet_id" {
  description = "CDC VNet ID (peer the database VNets to this)."
  value       = azurerm_virtual_network.cdc.id
}

output "aks_subnet_id" {
  description = "AKS node subnet ID."
  value       = azurerm_subnet.aks.id
}

output "acr_login_server" {
  description = "ACR login server -- use as the host in aks/values.local.yaml connectImage."
  value       = azurerm_container_registry.cdc.login_server
}

output "acr_name" {
  description = "ACR name."
  value       = azurerm_container_registry.cdc.name
}

output "acr_push_token_name" {
  description = "ACR token username for the Strimzi Connect image push secret."
  value       = azurerm_container_registry_token.connect_push.name
}

output "acr_push_token_password" {
  description = "ACR token password for kubectl create secret docker-registry acr-push-credentials."
  value       = azurerm_container_registry_token_password.connect_push.password1[0].value
  sensitive   = true
}

output "key_vault_name" {
  description = "Existing Key Vault name -- set as the keyVault.name for any instance in aks/values.local.yaml that uses this vault."
  value       = data.azurerm_key_vault.cdc.name
}

output "key_vault_uri" {
  description = "Existing Key Vault URI."
  value       = data.azurerm_key_vault.cdc.vault_uri
}

output "tenant_id" {
  description = "Azure AD tenant ID -- set as the keyVault.tenantId for any instance in aks/values.local.yaml that uses this vault."
  value       = data.azurerm_client_config.current.tenant_id
}

output "keyvault_csi_client_id" {
  description = "Key Vault CSI add-on identity clientId -- set as the keyVault.identityClientId for any instance in aks/values.local.yaml that uses this vault."
  value       = azurerm_kubernetes_cluster.cdc.key_vault_secrets_provider[0].secret_identity[0].client_id
}

output "cdc_node_label" {
  description = "Label on the CDC node pool (workload=cdc-rollback). Optional nodeSelector for the chart."
  value       = "workload=cdc-rollback"
}

output "helm_values_snippet" {
  description = "Non-secret fields for aks/values.local.yaml produced by this stack. Paste the keyVault block into each instances[] entry that uses this vault (instances can use other vaults too)."
  value       = <<-EOT
    connectImage: ${azurerm_container_registry.cdc.login_server}/cdc-kafka-connect:3.9-cdc1
    # Replace placeholders in aks/values.local.yaml (this is not a full values file):
    #   keyVault:
    #     name: ${data.azurerm_key_vault.cdc.name}
    #     tenantId: ${data.azurerm_client_config.current.tenant_id}
    #     identityClientId: ${azurerm_kubernetes_cluster.cdc.key_vault_secrets_provider[0].secret_identity[0].client_id}
  EOT
}

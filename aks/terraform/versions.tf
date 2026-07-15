terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      # 4.42+ required for rbac_authorization_enabled on azurerm_key_vault
      version = ">= 4.42.0, < 5.0.0"
    }
  }
}

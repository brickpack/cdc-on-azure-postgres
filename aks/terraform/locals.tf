data "azurerm_client_config" "current" {}

locals {
  # Globally unique-ish names. ACR must be alphanumeric only.
  rg_name   = "${var.name_prefix}-aks-rg"
  vnet_name = "${var.name_prefix}-aks-vnet"
  aks_name  = "${var.name_prefix}-aks"
  acr_name  = replace("${var.name_prefix}cdcacr${substr(md5(var.name_prefix), 0, 6)}", "-", "")

  # Azure CNI Overlay: nodes take IPs from the subnet; pods use a separate
  # overlay CIDR. service_cidr must NOT overlap any peered VNet (dbmig-dev
  # uses 10.0.0.0/16) -- keep it in 172.16/12.
  pod_cidr       = "10.244.0.0/16"
  service_cidr   = "172.16.0.0/16"
  dns_service_ip = "172.16.0.10"
}

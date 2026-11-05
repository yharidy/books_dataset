locals {
  catalog_layers = {
    for pair in setproduct(var.catalogs, var.layers) : "${pair[0]}.${pair[1]}" => {
      catalog = pair[0]
      layer   = pair[1]
    }
  }
}

resource "databricks_schema" "layer" {
  for_each     = local.catalog_layers
  catalog_name = each.value.catalog
  name         = each.value.layer
}

resource "databricks_volume" "landing" {
  for_each     = toset(var.catalogs)
  catalog_name = each.key
  schema_name  = databricks_schema.layer["${each.key}.bronze"].name
  name         = "landing"
  volume_type  = "MANAGED"
}

resource "databricks_volume" "checkpoints" {
  for_each     = local.catalog_layers
  catalog_name = each.value.catalog
  schema_name  = databricks_schema.layer[each.key].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
}

resource "databricks_volume" "schemas" {
  for_each     = local.catalog_layers
  catalog_name = each.value.catalog
  schema_name  = databricks_schema.layer[each.key].name
  name         = "schemas"
  volume_type  = "MANAGED"
}


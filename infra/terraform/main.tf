resource "databricks_schema" "layer" {
  for_each     = toset(var.layers)
  catalog_name = var.catalog_name
  name         = each.key
}

resource "databricks_volume" "landing" {
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.layer["bronze"].name
  name         = "landing"
  volume_type  = "MANAGED"
}

resource "databricks_volume" "checkpoints" {
  for_each     = toset(var.layers)
  catalog_name = var.catalog_name
  schema_name  = databricks_schema.layer[each.key].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
}

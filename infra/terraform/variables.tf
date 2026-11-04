variable "databricks_profile" {
  type        = string
  description = "Profile name in ~/.databrickscfg"
  default     = "free"
}

variable "layers" {
  default = ["bronze", "silver", "gold"]
}

variable "catalogs" {
  type        = list(string)
  description = "List of existing catalogs to create schemas in"
}

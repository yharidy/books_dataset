# Terraform

Run Terraform from `infra/terraform`:

```bash
cd infra/terraform
terraform init
terraform plan -var-file=terrafrom.tfvars
terraform apply -var-file=terrafrom.tfvars
```
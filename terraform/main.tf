terraform {
  required_providers {
    oci = {
      source = "oracle/oci"
      version = "6.9.0"
    }
  }
}

provider "oci" {
  tenancy_ocid = var.tenancy_ocid
  user_ocid = var.user_ocid
  private_key_path = var.private_key_path
  fingerprint = var.fingerprint
  region = var.region
}

resource "oci_objectstorage_bucket" "prova_bucket1" {
    #Required
    compartment_id = var.compartment_id
    name = var.bucket_name
    namespace = var.namespace
}
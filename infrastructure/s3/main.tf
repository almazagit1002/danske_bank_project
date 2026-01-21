terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

# --- S3 Bucket ---
resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.bucket_name

  tags = {
    Name        = "Danske Bank Project Raw Data"
    Environment = "dev"
  }

  # ACL removed because AWS now blocks ACLs on new buckets
}

# --- Folders inside S3 bucket ---
locals {
  folders = [
    "payments_core/",
    "fraud_system/",
    "merchant_system/",
    "device_telemetry/",
    "customer_behavior/",
    "fraud_timing/",
    "data_source/"
  ]
}

resource "aws_s3_object" "folders" {
  for_each = toset(local.folders)
  bucket   = aws_s3_bucket.raw_data_bucket.bucket
  key      = each.key
}

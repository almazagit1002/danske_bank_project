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

# --- Raw Data S3 Bucket ---
resource "aws_s3_bucket" "raw_data_bucket" {
  bucket = var.raw_bucket_name

  tags = {
    Name        = "Danske Bank Project Raw Data"
    Environment = "dev"
  }
}



# --- Metadata S3 Bucket ---
resource "aws_s3_bucket" "metadata_bucket" {
  bucket = var.metadata_bucket_name

  tags = {
    Name        = "Danske Bank Project Metadata"
    Environment = "dev"
  }
}




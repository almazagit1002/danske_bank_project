output "raw_bucket_name" {
  value = aws_s3_bucket.raw_data_bucket.bucket
}

output "metadata_bucket_name" {
  value = aws_s3_bucket.metadata_bucket.bucket
}

import boto3
from pathlib import Path
import pandas as pd
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class S3Uploader:
    """
    Class to upload local data folders to S3 with row counting and safe overwrite.
    """

    def __init__(self, bucket_name: str, base_path: str):
        self.bucket_name = bucket_name
        self.base_path = Path(base_path)
        self.s3 = boto3.client("s3")

        # Define S3 folder mapping
        self.s3_prefixes = {
            "payments_core": "payments_core",
            "merchant_system": "merchant_system",
            "device_telemetry": "device_telemetry",
            "customer_behavior": "customer_behavior",
            "fraud_timing": "fraud_timing"
        }

    def upload_folder(self, local_path: Path, s3_prefix: str):
        """
        Upload all files from a local folder to S3 under a given prefix.
        Clears existing objects under that prefix before upload.
        Counts rows in each file for logging.
        """
        try:
            logging.info(f"Clearing objects under s3://{self.bucket_name}/{s3_prefix} ...")
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=s3_prefix)

            if "Contents" in response:
                objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
                self.s3.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects_to_delete})
                logging.info(f"Deleted {len(objects_to_delete)} objects from s3://{self.bucket_name}/{s3_prefix}")
            else:
                logging.info("No objects to delete.")

            # Upload files
            files_to_upload = [f for f in local_path.rglob("*") if f.is_file()]
            total_rows = 0

            for file_path in files_to_upload:
                try:
                    # Count rows
                    if file_path.suffix == ".parquet":
                        df_file = pd.read_parquet(file_path)
                        row_count = len(df_file)
                    else:
                        with open(file_path, "r", encoding="utf-8") as f:
                            row_count = sum(1 for _ in f)

                    total_rows += row_count
                    logging.info(f"{file_path.name}: {row_count} rows")

                    # Upload
                    s3_key = f"{s3_prefix}/{file_path.name}"
                    logging.info(f"Uploading {file_path}  s3://{self.bucket_name}/{s3_key}")
                    self.s3.upload_file(Filename=str(file_path), Bucket=self.bucket_name, Key=s3_key)
                except Exception as file_err:
                    logging.error(f"Failed to upload {file_path}: {file_err}")

            logging.info(f"Upload complete for {s3_prefix}. Total rows uploaded: {total_rows}")

        except Exception as e:
            logging.error(f"Failed to upload folder {local_path} to S3 prefix {s3_prefix}: {e}")

    def run_all(self):
        """
        Upload all configured folders to S3.
        """
        logging.info("Starting S3 upload process...")
        for folder_name, s3_prefix in self.s3_prefixes.items():
            local_folder = self.base_path / folder_name
            if not local_folder.exists():
                logging.warning(f"Skipping missing folder: {local_folder}")
                continue
            self.upload_folder(local_folder, s3_prefix)
        logging.info("All uploads completed successfully.")


def main():
    uploader = S3Uploader(bucket_name="danske-bank-project", base_path="data/raw")
    uploader.run_all()


if __name__ == "__main__":
    main()

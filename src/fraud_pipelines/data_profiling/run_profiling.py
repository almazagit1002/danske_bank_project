import yaml
import boto3
import logging
from pyspark.sql.functions import current_timestamp
from utils.spark_session import get_spark
from utils.profiler import profile_dataframe
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


class BronzeProfiler:
    """
    Profiles datasets in the Bronze layer and writes metrics to S3 metadata bucket.
    Skips empty sources to avoid errors.
    """

    def __init__(self, config_path: str):
        self.config_path = config_path

        # Load YAML config
        try:
            with open(self.config_path, "r") as f:
                cfg = yaml.safe_load(f)

            self.bronze_base_path = cfg["BRONZE_BASE_PATH"]
            self.metadata_base_path = cfg["METADATA_BASE_PATH"]
            self.region = cfg.get("region", "us-east-1")
            self.sources = cfg.get("sources", [])

            logging.info(f"Loaded configuration for {len(self.sources)} sources")
            logging.info(f"Bronze path: {self.bronze_base_path}, Metadata path: {self.metadata_base_path}, Region: {self.region}")

        except Exception as e:
            logging.error(f"Failed to load config file {self.config_path}: {e}")
            raise

        # Initialize Spark session
        self.spark = get_spark("Bronze Data Profiling")

        # Initialize S3 client
        self.s3 = boto3.client("s3", region_name=self.region)

        # Ensure metadata bucket exists
        self.ensure_bucket(self.get_bucket_name(self.metadata_base_path))

    @staticmethod
    def get_bucket_name(s3_path: str) -> str:
        """
        Extract bucket name from S3 path.
        """
        return s3_path.replace("s3a://", "").split("/")[0]

    def path_exists(self, s3_path: str) -> bool:
        """
        Check if there is at least one object under the S3 prefix.
        """
        try:
            parsed = urlparse(s3_path)
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")

            response = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            return "Contents" in response
        except Exception as e:
            logging.warning(f"Failed to check path {s3_path}: {e}")
            return False

    def ensure_bucket(self, bucket_name: str):
        """
        Create bucket if it does not exist.
        """
        try:
            existing = [b["Name"] for b in self.s3.list_buckets()["Buckets"]]
            if bucket_name not in existing:
                self.s3.create_bucket(Bucket=bucket_name)
                logging.info(f"Created bucket: {bucket_name}")
            else:
                logging.info(f"Bucket already exists: {bucket_name}")
        except Exception as e:
            logging.error(f"Failed to ensure bucket {bucket_name}: {e}")
            raise

    def profile_source(self, source: str):
        """
        Profile a single Bronze dataset and write metadata to S3.
        Skips if the source path is empty.
        """
        s3_path = f"{self.bronze_base_path}/{source}"
        if not self.path_exists(s3_path):
            logging.warning(f"Skipping source {source}: path {s3_path} is empty or does not exist")
            return

        try:
            logging.info(f"Profiling source: {source}")
            df = self.spark.read.parquet(s3_path)

            profile_df = profile_dataframe(df, source) \
                .withColumn("profiling_timestamp", current_timestamp())

            output_path = f"{self.metadata_base_path}/source={source}"
            profile_df.write.mode("overwrite").parquet(output_path)
            logging.info(f"Profiling written to {output_path}")

        except Exception as e:
            logging.error(f"Failed to profile source {source}: {e}")

    def run(self):
        """
        Run profiling for all configured sources.
        """
        logging.info("Starting Bronze profiling pipeline...")
        for source in self.sources:
            self.profile_source(source)
        logging.info("Bronze profiling pipeline completed.")

    def stop(self):
        """
        Stop the Spark session.
        """
        self.spark.stop()
        logging.info("Spark session stopped.")


def main():
    profiler = BronzeProfiler(
        config_path="config/profiling.yaml",
    )
    profiler.run()
    profiler.stop()


if __name__ == "__main__":
    main()

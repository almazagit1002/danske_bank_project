import boto3
import io
import yaml
import logging
from pyspark.sql.functions import lit, current_timestamp, col
from pyspark.sql.types import TimestampType
from utils.spark_session import get_spark  

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class BronzePipeline:
    """
    Ingests raw datasets into Bronze layer in S3 using Spark.
    Supports CSV, Parquet, JSON, JSONL, and YAML sources.
    Adds ingestion tracking columns and ensures timestamp consistency.
    """

    def __init__(self, config_path: str, bronze_base_path: str):
        self.config_path = config_path
        self.bronze_base_path = bronze_base_path
        self.spark = get_spark("Bronze Ingestion Pipeline")

        # Load config
        try:
            with open(self.config_path, "r") as f:
                self.cfg_yaml = yaml.safe_load(f)
            self.sources = self.cfg_yaml.get("sources", {})
            logging.info(f"Loaded configuration for {len(self.sources)} sources")
        except Exception as e:
            logging.error(f"Failed to load config file {self.config_path}: {e}")
            raise

    def extract_source(self, source_name: str, cfg: dict):
        """
        Extract a source dataset into a Spark DataFrame, with tracking columns.
        Supports CSV, Parquet, JSON, JSONL.
        """
        try:
            path = cfg["path"]
            fmt = cfg["format"].lower()


            if fmt in ["json", "jsonl"]:
                reader = self.spark.read.format("json")
                if fmt == "json":
                    reader = reader.option("multiLine", cfg.get("multiLine", False))
                df = reader.load(path)
                if df.rdd.isEmpty():
                    raise ValueError(f"Source {source_name} is empty")

            else:  # csv, parquet, etc.
                reader = self.spark.read.format(fmt)
                if fmt == "csv":
                    reader = reader.option("header", True).option("inferSchema", True)
                df = reader.load(path)
                if df.rdd.isEmpty():
                    raise ValueError(f"Source {source_name} is empty")

            # Add tracking columns
            df = df.withColumn("source_system", lit(source_name)) \
                   .withColumn("ingestion_timestamp", current_timestamp())

            # Cast all timestamp columns to Spark TimestampType (millisecond precision)
            for c, t in df.dtypes:
                if t.startswith("timestamp"):
                    df = df.withColumn(c, col(c).cast(TimestampType()))

            row_count = df.count()
            logging.info(f"{source_name}: {row_count} rows read from source")
            return df, row_count

        except Exception as e:
            logging.error(f"Failed to extract source {source_name}: {e}")
            raise

    def write_bronze(self, df, source_name: str):
        """
        Write a Spark DataFrame to the Bronze layer in S3, overwriting old data.
        """
        try:
            output_path = f"{self.bronze_base_path}/{source_name}/"
            logging.info(f"Writing {source_name} to Bronze: {output_path}")

            df.repartition(1).write \
                .mode("overwrite") \
                .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS") \
                .parquet(output_path)

            row_count = df.count()
            logging.info(f"Successfully ingested {source_name}: {row_count} rows uploaded")
        except Exception as e:
            logging.error(f"Failed to write {source_name} to Bronze: {e}")
            raise

    def run(self):
        """
        Run the Bronze ingestion pipeline for all sources in the config.
        """
        logging.info("Starting Bronze ingestion pipeline...")
        for source_name, cfg in self.sources.items():
            try:
                logging.info(f"Extracting source: {source_name}")
                df, rows_read = self.extract_source(source_name, cfg)
                self.write_bronze(df, source_name)
            except ValueError as ve:
                logging.warning(f"Skipping source {source_name}: {ve}")
            except Exception as e:
                logging.error(f"Failed to process source {source_name}: {e}")

        logging.info("Bronze ingestion pipeline completed.")


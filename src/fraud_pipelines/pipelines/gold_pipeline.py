import yaml
import logging
from typing import Dict

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, TimestampType
from utils.spark_session import get_spark


class GoldPipeline:
    """
    Gold layer ingestion pipeline for transaction-centric data.
    """

    def __init__(
        self,
        config_path: str = "config/golden_config.yaml",
    ):
        self.config_path = config_path
        self.dfs: Dict[str, object] = {}
   
        # Logging and Spark session
        self._configure_logging()
        self._load_configs()
        self.spark = get_spark("Gold Ingestion")

    def _configure_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        self.logger = logging.getLogger(self.__class__.__name__)

    def _load_configs(self):
        try:
            with open(self.config_path) as f:
                cfg = yaml.safe_load(f)

            self.silver_base_path = cfg["paths"]["silver"]
            self.gold_base_path = cfg["paths"]["gold"]
            self.sources = cfg.get("sources")
            self.transaction_sources= cfg.get("transaction_sources")

            self.logger.info("Configuration loaded. Sources: %s", self.sources)

        except Exception as e:
            self.logger.exception("Failed to load configuration")
            raise


    def load_silver_data(self):
        self.logger.info("Loading Silver datasets...")
        for src in self.sources:
            try:
                df = self.spark.read.parquet(f"{self.silver_base_path}/{src}")
                count = df.count()
                self.dfs[src] = df
                self.logger.info("%s loaded with %d rows", src, count)
            except Exception as e:
                self.logger.exception("Failed to load Silver dataset for %s", src)
                self.dfs[src] = None

    def join_transaction_sources(self):
        gold_df = self.dfs.get("payments")
        if gold_df is None:
            self.logger.error("Payments dataset missing, cannot join transaction sources")
            return None

        for src in self.transaction_sources[1:]:
            join_df = self.dfs.get(src)
            if join_df is None:
                self.logger.warning("%s dataset missing, skipping join", src)
                continue
            cols_to_add = [c for c in join_df.columns if c not in gold_df.columns or c == "transaction_id"]
            gold_df = gold_df.join(join_df.select(*cols_to_add), on="transaction_id", how="left")

        self.logger.info(
            "After transaction_id joins: %d rows, %d columns",
            gold_df.count(), len(gold_df.columns)
        )
        return gold_df

    def join_customer_behavior(self, gold_df):
        cb_df = self.dfs.get("customer_behavior")
        if cb_df is None:
            self.logger.warning("customer_behavior dataset missing, skipping join")
            return gold_df
        cb_df = cb_df.drop("source_system", "ingestion_timestamp")
        gold_df = gold_df.join(cb_df, on="sender_account", how="left")
        self.logger.info(
            "After customer_behavior join: %d rows, %d columns",
            gold_df.count(), len(gold_df.columns)
        )
        return gold_df

    def standardize_columns(self, gold_df):
        if gold_df is None:
            self.logger.warning("standardize_columns called with None dataframe")
            return None

        for ts_col in [c for c in gold_df.columns if "time" in c or "timestamp" in c]:
            gold_df = gold_df.withColumn(ts_col, F.col(ts_col).cast(TimestampType()))
            gold_df = gold_df.withColumn(ts_col, F.from_utc_timestamp(F.col(ts_col), "UTC"))

        for amt_col in ["amount"]:
            if amt_col in gold_df.columns:
                gold_df = gold_df.withColumn(amt_col, F.col(amt_col).cast(DecimalType(18, 2)))

        self.logger.info("Columns standardized: %d columns", len(gold_df.columns))
        return gold_df
    

    def add_row_hash(self, gold_df):
        if gold_df is None:
            self.logger.warning("add_row_hash called with None dataframe")
            return None

        hash_cols = [c for c in gold_df.columns if c != "row_hash"]
        gold_df = gold_df.withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.col(c).cast(StringType()) for c in hash_cols]), 256)
        )
        return gold_df


    def write_gold_dataset(self, gold_df, name: str):
        try:
            path = f"{self.gold_base_path}/{name}"
            gold_df.write.mode("overwrite").parquet(path)
            self.logger.info("%s dataset written: %d rows", name, gold_df.count())
        except Exception as e:
            self.logger.exception("Failed to write Gold dataset: %s", name)

    def generate_analytics(self, gold_df):
        """
        Generate aggregated analytics datasets for Gold layer:
        - Fraud by Category
        - Fraud by Type
        - Fraud by Location
        - Device risk
        Includes timestamp and fraud_type columns in each dataset.
        """
        try:
            # Fraud by Category
            fraud_category_df = gold_df.groupBy("Category").agg(
                F.countDistinct("transaction_id").alias("fraud_transaction_count"),
                F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
                F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount"),
                F.collect_set("timestamp").alias("timestamps"),
                F.collect_set("fraud_type").alias("fraud_types")
            ).withColumn(
                "dataset_type", F.lit("fraud_by_category")
            ).withColumn(
                "profiling_timestamp", F.current_timestamp()
            )
            self.write_gold_dataset(fraud_category_df, "fraud_by_category")

            # Fraud by Type
            fraud_type_df = gold_df.groupBy("Type").agg(
                F.countDistinct("transaction_id").alias("fraud_transaction_count"),
                F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
                F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount"),
                F.collect_set("timestamp").alias("timestamps"),
                F.collect_set("fraud_type").alias("fraud_types")
            ).withColumn(
                "dataset_type", F.lit("fraud_by_type")
            ).withColumn(
                "profiling_timestamp", F.current_timestamp()
            )
            self.write_gold_dataset(fraud_type_df, "fraud_by_type")

            # Fraud by Location
            fraud_location_df = gold_df.groupBy("location").agg(
                F.countDistinct("transaction_id").alias("fraud_transaction_count"),
                F.sum(F.col("is_fraud").cast("int")).alias("fraud_count"),
                F.sum(F.when(F.col("is_fraud") == 1, F.col("amount")).otherwise(0)).alias("fraud_amount"),
                F.collect_set("timestamp").alias("timestamps"),
                F.collect_set("fraud_type").alias("fraud_types")
            ).withColumn(
                "dataset_type", F.lit("fraud_by_location")
            ).withColumn(
                "profiling_timestamp", F.current_timestamp()
            )
            self.write_gold_dataset(fraud_location_df, "fraud_by_location")

          
        except Exception:
            self.logger.exception("Failed to generate analytics datasets")

    def run(self):
        self.logger.info("Starting Gold ingestion pipeline")

        # Load Silver datasets
        self.load_silver_data()

        # Join transaction sources
        gold_df = self.join_transaction_sources()
        if gold_df is None:
            self.logger.error("Gold DataFrame could not be created from transaction sources. Exiting pipeline.")
            return

        # Join customer_behavior
        gold_df = self.join_customer_behavior(gold_df)
        if gold_df is None:
            self.logger.error("Gold DataFrame is None after customer_behavior join. Exiting pipeline.")
            return

        # Standardize columns
        gold_df = self.standardize_columns(gold_df)
        if gold_df is None:
            self.logger.error("Gold DataFrame is None after standardization. Exiting pipeline.")
            return

        # Add row hash
        gold_df = self.add_row_hash(gold_df)
        if gold_df is None:
            self.logger.error("Gold DataFrame is None after adding row hash. Exiting pipeline.")
            return

        # Write main Gold dataset
        self.write_gold_dataset(gold_df, "fraud_detection")

        # Generate analytics
        self.generate_analytics(gold_df)

        self.spark.stop()
        self.logger.info("Spark session stopped.")



def main():
    pipeline = GoldPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()

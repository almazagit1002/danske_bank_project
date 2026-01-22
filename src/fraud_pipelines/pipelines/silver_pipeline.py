import yaml
import logging
from typing import Dict

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DecimalType
from utils.spark_session import get_spark


class SilverPipeline:
    """
    Silver layer ingestion and standardization pipeline.
    """

    def __init__(
        self,
        config_path: str = "config/silver_config.yaml",
    ):
        self.config_path = config_path
      

        self.alerts = []
        self.dfs: Dict[str, object] = {}
        self.row_counts: Dict[str, int] = {}

        self._configure_logging()
        self._load_configs()
        self.spark = get_spark("Silver Ingestion")

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

            self.bronze_base_path = cfg["paths"]["bronze"]
            self.silver_base_path = cfg["paths"]["silver"]
            self.standardize_cols = cfg.get("standardize_cols", {})
            self.null_location_threshold = cfg.get("null_location_threshold", 0.05)
            self.sources = cfg.get("sources")

            self.logger.info(
                "Configuration loaded. Sources=%s", len(self.sources)
            )

        except Exception as e:
            self.logger.exception("Failed to load configuration")
            raise

    
    # Transformations
    def standardize_columns(self, df):
        for old, new in self.standardize_cols.items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        return df

    def cast_columns(self, df):
        if "event_time" in df.columns:
            df = df.withColumn("event_time", F.col("event_time").cast(TimestampType()))

        if "timestamp" in df.columns:
            df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))

        numeric_cols = ["amount", "avg_amount_last_24h", "max_velocity_score",
                        "mean_velocity_score", "min_velocity_score"]
        for col in numeric_cols:
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.when(
                        F.col(col).cast("double").isNotNull(),
                        F.round(F.col(col).cast(DecimalType(18, 2)), 2)
                    ).otherwise(None)
                )
        return df

    def split_device_dict(self, df):
        if "device" in df.columns:
            df = (
                df.withColumn("device_hash", F.col("device").getItem("device_hash"))
                  .withColumn("device_used", F.col("device").getItem("device_used"))
                  .drop("device")
            )

        if "network" in df.columns:
            df = df.withColumn("ip_address", F.col("network").getItem("ip_address")) \
                   .drop("network")
        return df

    def add_derivations(self, df):
        if "location" in df.columns:
            df = df.withColumn("location_missing", F.col("location").isNull())

        if {"fraud_type", "is_fraud"}.issubset(df.columns):
            df = df.withColumn(
                "is_fraud_inconsistent",
                (F.col("is_fraud") == True) & F.col("fraud_type").isNull()
            )
        return df

    
    # Quality checks
    def uniqueness_check(self, df, col: str, source: str):
        if col not in df.columns:
            return
        total = df.count()
        distinct = df.select(col).distinct().count()
        if total != distinct:
            self.alerts.append(
                f"{source}: {col} not unique ({total} rows vs {distinct} distinct)"
            )

    def column_completeness(self, df, source: str):
        total = df.count()
        for col in df.columns:
            nulls = df.filter(F.col(col).isNull()).count()
            pct = nulls / total if total else 0
            if pct > self.null_location_threshold:
                self.alerts.append(
                    f"{source}: {col} has {pct*100:.2f}% nulls"
                )

    def row_count_consistency_check(self):
        if not self.row_counts:
            return
        counts = list(self.row_counts.values())
        empty_sources = [s for s, c in self.row_counts.items() if c == 0]
        if empty_sources:
            self.alerts.append(f"Empty datasets detected: {', '.join(empty_sources)}")
        if len(set(counts)) > 1:
            details = ", ".join(f"{s}={c}" for s, c in self.row_counts.items())
            self.alerts.append(f"Row count mismatch across sources: {details}")

    
    # Main processing
    def process_source(self, source: str):
        try:
            self.logger.info("Processing source: %s", source)
            df = self.spark.read.parquet(f"{self.bronze_base_path}/{source}")
            df.cache()  # Optional caching for performance
            self.row_counts[source] = df.count()

            df = self.standardize_columns(df)
            df = self.cast_columns(df)
            df = self.split_device_dict(df)
            df = self.add_derivations(df)

            self.uniqueness_check(df, "transaction_id", source)
            self.column_completeness(df, source)

            self.dfs[source] = df
            output_path = f"{self.silver_base_path}/{source}"
            df.write.mode("overwrite").parquet(output_path)

            self.logger.info("Silver data written to %s", output_path)

        except Exception:
            self.logger.exception("Failed processing source %s", source)
            self.alerts.append(f"{source}: processing failed")

    def cross_source_validation(self):
        try:
            if "payments" in self.dfs:
                base_tx = set(
                    self.dfs["payments"].select("transaction_id")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )
                for src in ["fraud_system", "device"]:
                    if src not in self.dfs:
                        continue
                    tx_ids = set(self.dfs[src].select("transaction_id").rdd.flatMap(lambda x: x).collect())
                    missing = base_tx - tx_ids
                    if missing:
                        self.alerts.append(f"{src}: {len(missing)} transaction_ids missing vs payments")

            if {"customer_behavior", "payments"}.issubset(self.dfs):
                cb = set(self.dfs["customer_behavior"].select("sender_account").rdd.flatMap(lambda x: x).collect())
                pm = set(self.dfs["payments"].select("sender_account").rdd.flatMap(lambda x: x).collect())
                missing = pm - cb
                if missing:
                    self.alerts.append(f"customer_behavior: {len(missing)} sender_accounts missing vs payments")
        except Exception:
            self.logger.exception("Cross-source validation failed")

    
    # Pipeline runner
    def run(self):
        self.logger.info("Starting Silver ingestion pipeline")
        for source in self.sources:
            self.process_source(source)
        self.row_count_consistency_check()
        self.cross_source_validation()

        if self.alerts:
            self.logger.warning("Alerts detected:")
            for alert in self.alerts:
                self.logger.warning(alert)
        else:
            self.logger.info("No alerts detected")

        self.spark.stop()
        self.logger.info("Spark session stopped")



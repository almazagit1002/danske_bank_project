import logging
import sys

from fraud_pipelines.pipelines.bronze_pipeline import BronzePipeline
from fraud_pipelines.pipelines.silver_pipeline import SilverPipeline
from fraud_pipelines.pipelines.gold_pipeline import GoldPipeline
from fraud_pipelines.data_analysis.fraud_stats import FraudStatsAnalyzer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("FullPipeline")

def main():
    logger.info("Starting full data pipeline...")

    # Bronze ingestion
    try:
        logger.info("Starting Bronze ingestion pipeline...")
        bronze_pipeline = BronzePipeline(
            config_path="config/bronze_config.yaml",
            bronze_base_path="s3a://danske-bank-project/bronze"
        )
        bronze_pipeline.run()
        logger.info("Bronze ingestion pipeline finished successfully.")
    except Exception as e:
        logger.exception("Bronze ingestion pipeline failed: %s", e)

    #Silver ingestion

    try:
        logger.info("Starting Silver ingestion pipeline...")
        silver_pipeline = SilverPipeline(
            config_path="config/silver_config.yaml"
        )
        silver_pipeline.run()
        logger.info("Silver ingestion pipeline finished successfully.")
    except Exception as e:
        logger.exception("Silver ingestion pipeline failed: %s", e)

    #Gold ingestion
    try:
        logger.info("Starting Gold ingestion pipeline...")
        gold_pipeline = GoldPipeline(
            config_path="config/gold_config.yaml"
        )
        gold_pipeline.run()
        logger.info("Gold ingestion pipeline finished successfully.")
    except Exception as e:
        logger.exception("Gold ingestion pipeline failed: %s", e)

    logger.info("Full pipeline finished.")

    try:
        logger.info("Starting stats analyisis...")
        analyzer = FraudStatsAnalyzer("config/stats_config.yaml")
        analyzer.load_datasets()
        analyzer.plot_category_stats()
        analyzer.plot_location_stats()
        logger.info("Stats analyisis finished successfully.")
    except Exception as e:
        logger.exception("Stats analyisis failed: %s", e)

    logger.info("Full pipeline finished.")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Pipeline execution failed: %s", e)

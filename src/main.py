import logging
import sys

from fraud_pipelines.pipelines.bronze_pipeline import BronzePipeline
from fraud_pipelines.pipelines.silver_pipeline import SilverPipeline
from fraud_pipelines.pipelines.gold_pipeline import GoldPipeline
from fraud_pipelines.data_analysis.fraud_stats import FraudStatsAnalyzer
from utils.timed_stage import timed_stage


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
        with timed_stage("Bronze ingestion pipeline", logger):
            bronze_pipeline = BronzePipeline(
                config_path="config/bronze_config.yaml",
                bronze_base_path="s3a://danske-bank-project/bronze"
            )
            bronze_pipeline.run()
    except Exception:
        logger.exception("Bronze ingestion pipeline failed.")

    # Silver ingestion
    try:
        with timed_stage("Silver ingestion pipeline", logger):
            silver_pipeline = SilverPipeline(
                config_path="config/silver_config.yaml"
            )
            silver_pipeline.run()
    except Exception:
        logger.exception("Silver ingestion pipeline failed.")

    # Gold ingestion
    try:
        with timed_stage("Gold ingestion pipeline", logger):
            gold_pipeline = GoldPipeline(
                config_path="config/gold_config.yaml"
            )
            gold_pipeline.run()
    except Exception:
        logger.exception("Gold ingestion pipeline failed.")

    # Stats / analysis
    try:
        with timed_stage("Fraud statistics analysis", logger):
            analyzer = FraudStatsAnalyzer("config/stats_config.yaml")
            analyzer.load_datasets()
            analyzer.plot_category_stats()
            analyzer.plot_location_stats()
    except Exception:
        logger.exception("Stats analysis failed.")

    logger.info("Full pipeline finished.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Pipeline execution failed: %s", e)

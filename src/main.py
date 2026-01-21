# main.py
from fraud_pipelines.pipelines.bronze_pipeline import BronzePipeline
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    logging.info("Starting full data pipeline...")

    # Bronze ingestion
    bronze_pipeline = BronzePipeline(
        config_path="config/bronze_config.yaml",
        bronze_base_path="s3a://danske-bank-project/bronze"
    )
    bronze_pipeline.run()

    logging.info("Pipeline finished successfully.")

if __name__ == "__main__":
    main()

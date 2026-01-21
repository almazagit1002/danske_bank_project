import pandas as pd
import numpy as np
from pathlib import Path
import json
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

class DataDecompose:
    """
    Class to split a large fraud dataset into multiple domain-specific files.
    Generates:
        - payments_core.csv
        - merchant_transactions.parquet
        - customer_behavior.csv
        - fraud_timing.parquet
        - device_events.jsonl
    """
    
    def __init__(self, input_csv: str, base_path: str = "data/raw"):
        self.input_csv = Path(input_csv)
        self.base_path = Path(base_path) 
        self.paths = {
            "payments": self.base_path / "payments_core",
            "merchant": self.base_path / "merchant_system",
            "behavior": self.base_path / "customer_behavior",
            "fraud_timing": self.base_path / "fraud_timing",
            "device": self.base_path / "device_telemetry",
        }

        # Create directories
        for path in self.paths.values():
            path.mkdir(parents=True, exist_ok=True)

        # Load dataset
        try:
            self.df = pd.read_csv(self.input_csv)
            logging.info(f"Loaded dataset with {len(self.df)} rows and {len(self.df.columns)} columns")
        except Exception as e:
            logging.error(f"Failed to load CSV: {e}")
            raise

    def split_payments(self):
        try:
            payments_df = self.df[[
                "transaction_id", "timestamp", "sender_account", "receiver_account",
                "amount", "payment_channel", "location"
            ]].rename(columns={
                "transaction_id": "tx_id",
                "timestamp": "event_time"
            })

            payments_df.to_csv(self.paths["payments"] / "payments_core.csv", index=False)
            logging.info(f"Payments core CSV written with {len(payments_df)} rows")
        except Exception as e:
            logging.error(f"Failed to split payments: {e}")

    def split_merchant(self):
        try:
            merchant_df = self.df[[
                "transaction_id", "merchant_category", "transaction_type",
                "amount", "location"
            ]].rename(columns={
                "transaction_id": "ID",
                "merchant_category": "Category",
                "transaction_type": "Type",
                "amount": "Amount",
                "location": "Location"
            })

            for col in merchant_df.select_dtypes(include=["object"]).columns:
                merchant_df[col] = merchant_df[col].astype(str)

            merchant_df.to_parquet(
                self.paths["merchant"] / "merchant_transactions.parquet",
                index=False, engine="pyarrow"
            )
            logging.info(f"Merchant Parquet written with {len(merchant_df)} rows")
        except Exception as e:
            logging.error(f"Failed to split merchant data: {e}")

    def split_customer_behavior(self):
        try:
            behavior_df = self.df.groupby("sender_account").agg(
                avg_amount_last_24h=("amount", "mean"),
                tx_count_last_24h=("transaction_id", "count"),
                max_velocity_score=("velocity_score", "max"),
                mean_velocity_score=("velocity_score", "mean"),
                min_velocity_score=("velocity_score", "min"),
                avg_geo_anomaly_score=("geo_anomaly_score", "mean"),
                avg_spending_deviation_score=("spending_deviation_score", "mean")
            ).reset_index()

            behavior_df.to_csv(self.paths["behavior"] / "customer_behavior.csv", index=False)
            logging.info(f"Customer behavior CSV written with {len(behavior_df)} rows")
        except Exception as e:
            logging.error(f"Failed to split customer behavior: {e}")

    def split_fraud_timing(self):
        try:
            time_df = self.df[['transaction_id', 'timestamp', 'location', 'is_fraud', 'fraud_type']].copy()
            time_df['timestamp'] = pd.to_datetime(time_df['timestamp'], errors='coerce')
            logging.info(f"{time_df['timestamp'].isna().sum()} timestamps could not be parsed")

            def get_time_of_day(hour):
                if 5 <= hour < 12:
                    return "morning"
                elif 12 <= hour < 18:
                    return "afternoon"
                else:
                    return "night"

            time_df['time_of_day'] = time_df['timestamp'].dt.hour.apply(get_time_of_day)
            time_df['day_of_week'] = time_df['timestamp'].dt.day_name()

            # Randomly remove 1% of locations
            mask = np.random.rand(len(time_df)) < 0.01
            time_df.loc[mask, 'location'] = None

            time_df['timestamp'] = time_df['timestamp'].astype('datetime64[ms]')

            output_file = self.paths["fraud_timing"] / "fraud_timing.parquet"
            if len(time_df) == 0:
                logging.warning("fraud_timing source has 0 rows — skipping Parquet write")
            else:
                time_df.to_parquet(output_file, index=False)
                logging.info(f"Fraud timing Parquet written with {len(time_df)} rows")
        except Exception as e:
            logging.error(f"Failed to split fraud timing: {e}")

    def split_device_events(self):
        try:
            device_df = self.df[['transaction_id', 'device_used', 'device_hash', 'ip_address']]
            output_file = self.paths["device"] / "device_events.jsonl"
            with open(output_file, "w") as f:
                for row in device_df.itertuples(index=False):
                    record = {
                        "transaction_id": row.transaction_id,
                        "device": {"device_used": row.device_used, "device_hash": row.device_hash},
                        "network": {"ip_address": row.ip_address}
                    }
                    f.write(json.dumps(record) + "\n")
            logging.info(f"Device events JSONL written with {len(device_df)} rows")
        except Exception as e:
            logging.error(f"Failed to split device events: {e}")

    def run_all(self):
        logging.info("Starting data splitting process...")
        self.split_payments()
        self.split_merchant()
        self.split_customer_behavior()
        self.split_fraud_timing()
        self.split_device_events()
        logging.info("All data splits completed successfully")

def main():
    splitter = DataDecompose(
        input_csv="data/source/financial_fraud_detection_dataset.csv",
        base_path="data/raw"
    )
    splitter.run_all()

if __name__ == "__main__":
    main()

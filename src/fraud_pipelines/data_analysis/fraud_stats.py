import boto3
import pandas as pd
import yaml
import io
import logging
from pathlib import Path
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt
import seaborn as sns


class FraudStatsAnalyzer:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.bucket = self.config["S3_BUCKET"]
        self.datasets = self.config["datasets"]

        self.s3 = boto3.client("s3")
        self.dataframes = {}

        BASE_DIR = Path(__file__).resolve().parents[3]
        self.output_dir = BASE_DIR / "data" / "stats"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.scaler = MinMaxScaler()
        self._setup_logging()


    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def _load_config(self, path: str) -> dict:
        try:
            with open(path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise RuntimeError(f"Failed to load config file: {e}")


    def load_datasets(self):
        for ds in self.datasets:
            prefix = f"gold/{ds}/"
            self.logger.info(f"Loading dataset: {ds}")

            try:
                paginator = self.s3.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

                files = []
                for page in pages:
                    files.extend(
                        obj["Key"]
                        for obj in page.get("Contents", [])
                        if obj["Key"].endswith(".parquet")
                    )

                if not files:
                    self.logger.warning(f"No parquet files found for {ds}")
                    continue

                dfs = []
                for file in files:
                    obj = self.s3.get_object(Bucket=self.bucket, Key=file)
                    dfs.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))

                self.dataframes[ds] = pd.concat(dfs, ignore_index=True)
                self.logger.info(f"{ds}: {len(self.dataframes[ds])} rows loaded")

            except Exception as e:
                self.logger.error(f"Failed loading {ds}: {e}")


    def _scale_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = ["fraud_transaction_count", "fraud_amount"]
        df = df.copy()
        df[numeric_cols] = self.scaler.fit_transform(df[numeric_cols])
        return df


    def plot_category_stats(self):
        df = self.dataframes.get("fraud_by_category")
        if df is None:
            self.logger.warning("fraud_by_category not loaded")
            return

        df_scaled = self._scale_numeric(df)

        self._save_barplot(
            df_scaled.sort_values("fraud_transaction_count", ascending=False),
            x="Category",
            y="fraud_transaction_count",
            title="Scaled Fraud Transactions by Category",
            ylabel="Scaled Fraud Transaction Count (0–1)",
            filename="fraud_by_category_transactions.png"
        )

        self._save_barplot(
            df_scaled.sort_values("fraud_amount", ascending=False),
            x="Category",
            y="fraud_amount",
            title="Scaled Fraud Amount by Category",
            ylabel="Scaled Fraud Amount (0–1)",
            filename="fraud_by_category_amount.png"
        )

    def plot_location_stats(self, top_n: int = 20):
        df = self.dataframes.get("fraud_by_location")
        if df is None:
            self.logger.warning("fraud_by_location not loaded")
            return

        df_scaled = self._scale_numeric(df)

        self._save_barplot(
            df_scaled.sort_values("fraud_transaction_count", ascending=False).head(top_n),
            x="location",
            y="fraud_transaction_count",
            title="Top Locations by Scaled Fraud Transactions",
            ylabel="Scaled Fraud Transaction Count (0–1)",
            filename="fraud_by_location_transactions.png",
            rotate_x=90,
            figsize=(12, 6)
        )

        self._save_barplot(
            df_scaled.sort_values("fraud_amount", ascending=False).head(top_n),
            x="location",
            y="fraud_amount",
            title="Top Locations by Scaled Fraud Amount",
            ylabel="Scaled Fraud Amount (0–1)",
            filename="fraud_by_location_amount.png",
            rotate_x=90,
            figsize=(12, 6)
        )


    def _save_barplot(
        self,
        df,
        x,
        y,
        title,
        ylabel,
        filename,
        rotate_x=45,
        figsize=(10, 6)
    ):
        try:
            plt.figure(figsize=figsize)
            sns.barplot(data=df, x=x, y=y)
            plt.xticks(rotation=rotate_x)
            plt.ylabel(ylabel)
            plt.title(title)
            plt.tight_layout()

            output_path = self.output_dir / filename
            plt.savefig(output_path.with_suffix(".png"), dpi=150)
            plt.close()

            self.logger.info(f"Saved plot: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to save plot {filename}: {e}")



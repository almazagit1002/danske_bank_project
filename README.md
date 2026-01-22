# Fraud Data Engineering Pipeline Project for Danske Bank

## Overview

This project implements a small end-to-end data engineering pipeline for fraud analytics, designed as an exercise for a Data Engineering interview.

The pipeline ingests a large, structured fraud detection dataset, processes it through a simplified medallion architecture (Bronze / Silver / Gold), and produces analytical datasets and basic insights suitable for fraud monitoring and reporting.

The focus is on clarity of design, data quality, and analytical readiness, rather than  complexity.

## Dataset
The source data is a public Kaggle fraud detection dataset: 
https://www.kaggle.com/datasets/aryan208/financial-transactions-dataset-for-fraud-detection

It contains structured transactioninformation such as:

* Transaction amounts

* Transaction types

* Fraud labels

* Timestamps and identifiers

To simulate a realistic ingestion scenario, the raw data is decomposed into multiple source files with different formats and shapes before ingestion
This decomposition logic can be found in the data_decomposition/ directory.
## Architecture & Design

The pipeline follows a simplified medallion data architecture:

### Bronze – Raw Analytical Storage

* Raw source data is ingested and converted into Parquet


* Metadata columns added (e.g. ingestion timestamp)


Purpose: preserve raw data in an analytics-friendly format.

### Silver – Clean & Standardized Data

* Column names standardized

* Data types fixed

* Null values and basic data quality issues handled

* Validation of key business rules

Purpose: produce a clean, reliable dataset for downstream analytics.

### Gold – Analytics & Insights

* Aggregated, business-focused datasets created for fraud analysis

Examples:

* Fraud by Catgeory

* Fraud by Location

* Fraud Amounts

A small number of plots are generated to illustrate insights

Purpose: enable reporting, dashboards, and fraud monitoring use cases.

## Tech Stack

* Python 3.12

* Pandas for transformations
* Spark 

* Parquet as the analytical storage format

* AWS S3 as the data lake storage

* Terraform for infrastructure provisioning

* Matplotlib for basic visualizations

## How to Run

### 1. Create and Activate a Virtual Environment:
   Create a Python virtual environment to isolate dependencies:
```
   python3.12 -m venv venv
   source venv/bin/activate
```
    
### 2. Install Project Dependencies:
   Install all required Python libraries:
    ```
   pip install -r requirements.txt
   ```
   or 
    ```
   pip install -e .

   ```
   This allows imports from src/ to work correctly.


### 3. Provision Infrastructure (AWS S3):
   The project uses Terraform to provision the S3 data lake.
   Navigate to the Terraform directory:
    ```
   cd infrastructure/s3

   ```
   Initialize and apply the Terraform configuration:
    ```
   terraform init
   terraform plan
   terraform apply


   ```

This creates the required S3 bucket structure for the Bronze, Silver, and Gold layers.

Note: AWS credentials must be configured locally  before running Terraform.

### 4. Decompose and Upload Source Data:
To simulate multiple upstream data sources, the original dataset is decomposed into smaller files and uploaded to S3.

Run the data decomposition logic:
    ```
    cd data_decomposition
    python data_decompose.py
    python s3_raw_uploader.py
    ```
This step:
    * Splits the original dataset

    * Generates multiple source files with different schemas

    * Uploads raw data to the S3 Bronze layer

### 5 Run the Data Pipelines (Bronze → Silver → Gold)
Return to the project root and run the main pipeline entry point:


    ```
   cd ..
   python src/main.py



   ```

### Outputs
After successful execution, the following outputs are produced:

* Cleaned and aggregated datasets in S3 (Bronze / Silver / Gold)

* Analytical summary datasets for fraud monitoring

* PNG plots illustrating fraud trends by category and location

## Limitations & Future Improvements

- No orchestration layer — tasks are executed manually
- Data quality checks are basic and rule-based
- No real-time or streaming ingestion

Potential future improvements:
- Introduce orchestration (Airflow)
- Add automated data quality validation
- Add aletring system
- Extend analytics to support ML-based fraud scoring
- Implemet automated analytics report using llms


## Project Structure

```
danske_bank_project/
│
├── README.md
├── LICENSE
├── requirements.txt
├── setup.py
│
├── data/
│   ├── raw/            # Raw data extracted or staged before processing
│   ├── source/         # Decomposed source files simulating upstream systems
│   └── stats/          # Generated analytical plots and statistics (PDF/PNG)
│
├── data_decomposition/
│   ├── data_decompose.py     # Logic to split and reshape the original dataset
│   └── s3_raw_uploader.py    # Uploads decomposed raw data to S3
│
├── infrastructure/
│   └── s3/
│       ├── main.tf           # S3 bucket and related resources
│       ├── variables.tf
│       └── outputs.tf
│
├── src/
   ── main.py               # Entry point for running pipelines
   │
   ├── config/
   │   ├── bronze_config.yaml
   │   ├── silver_config.yaml
   │   ├── gold_config.yaml
   │   ├── stats_config.yaml
   │   └── profiling.yaml
   │
   ├── fraud_pipelines/
   │   ├── pipelines/
   │   │   ├── bronze_pipeline.py
   │   │   ├── silver_pipeline.py
   │   │   └── gold_pipeline.py
   │   │
   │   ├── data_analysis/
   │   │   ├── fraud_stats.py     # Analytical aggregations & plotting
   │   │   └── gold_stats.ipynb    # Exploratory analysis 
   │   │
   │   └── data_profiling/
   │       ├── run_profiling.py    # Dataset profiling logic
   │       └── profiles_analysis.ipynb
   │
   └── utils/
       ├── spark_session.py        # Spark session configuration
       └── profiler.py             # Reusable profiling utilities


```
# Fraud Data Engineering Pipeline Project for Danske Bank

## Overview

This project implements a small end-to-end data engineering pipeline for fraud analytics, designed as an exercise for a Data Engineering interview.

The pipeline ingests a large, structured fraud detection dataset, processes it through a simplified medallion architecture (Bronze / Silver / Gold), and produces analytical datasets and basic insights suitable for fraud monitoring and reporting.

The focus is on clarity of design, data quality, and analytical readiness, rather than  complexity.

## Dataset
The source data is a public Kaggle fraud detection dataset (´https://www.kaggle.com/datasets/aryan208/financial-transactions-dataset-for-fraud-detection´ )containing structured transactioninformation such as:

* Transaction amounts

* Transaction types

* Fraud labels

* Timestamps and identifiers

To simulate a realistic ingestion scenario, the raw data is decomposed into multiple source files with different formats and shapes before ingestion

## Architecture & Design

The pipeline follows a simplified medallion data architecture:

### Bronze – Raw Analytical Storage

* Raw source data is ingested and converted into Parquet


* Metadata columns added (e.g. ingestion timestamp)

* Stored in S3 under a /bronze prefix

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

* Fraud rate by transaction type

* Fraud rate by amount buckets

* Daily fraud volume

A small number of plots are generated to illustrate insights

Purpose: enable reporting, dashboards, and fraud monitoring use cases.

## Tech Stack

* Python 3.9+

* Pandas / NumPy for transformations

* Parquet as the analytical storage format

* AWS S3 as the data lake storage

* Terraform for infrastructure provisioning

* Matplotlib for basic visualizations
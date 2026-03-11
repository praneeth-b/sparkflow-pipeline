# Olist E-Commerce Data Pipeline using Apache Spark and orchestrated by Apache Airflow

A robust, local data engineering pipeline built with **Apache Airflow** and **Apache Spark**, deployed entirely on **Kubernetes** (via Docker Desktop). This project processes the [Kaggle Olist E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) through a strict **Medallion Architecture** (Bronze, Silver, Gold).

## 🏗 Architecture Overview

* **Orchestrator:** Apache Airflow 2.10.4
* **Execution Engine:** Apache Spark (PySpark 3.5.0) in Standalone Cluster Mode
* **Storage:** Local volume mounts simulating a Data Lake
* **Infrastructure:** Kubernetes (Local Docker Desktop K8s cluster)
* **Image:** Custom Fat Image supporting Java 17 and Python 3.12, optimized for Apple Silicon (ARM64).

## 🏅 The Medallion Architecture

The Airflow DAG (`olist_etl_dag.py`) triggers PySpark jobs that systematically refine the data through three distinct layers:

### 🥉 Bronze Layer (Raw Ingestion)

**Goal:** Create an immutable, highly compressed archive of the raw data while dropping completely corrupted records.

* **Operations:**
  * Reads the raw CSV files (`orders`, `order_items`, `order_reviews`, `sellers`).
  * Applies **strict schema enforcement** using Spark `StructType`.
  * Quarantines and drops malformed rows (`mode="DROPMALFORMED"`).
  * Converts the text-heavy CSVs into highly compressed, columnar **Parquet** format.

### 🥈 Silver Layer (Cleansed & Conformed)

**Goal:** Create the "Enterprise Source of Truth" by cleaning, deduplicating, and standardizing the data.

* **Operations:**
  * **Deduplication:** Drops duplicate records based on primary keys across all tables.
  * **Business Filtering:** Keeps only `delivered` or `shipped` orders and ensures item `price > 0`.
  * **Type Casting:** Converts raw string dates into native Spark `Timestamp` types for downstream date math.
  * **Imputation:** Handles missing data (e.g., defaulting missing review scores to `3`).

* *Note: A Branching Data Quality Gate exists between Silver and Gold in the DAG to alert on any critical data anomalies before aggregating.*

### 🥇 Gold Layer (Business Aggregations)

**Goal:** Provide highly aggregated, presentation-ready metrics directly designed for BI dashboards and executives.

* **Operations:**
  * **Feature Engineering:** Calculates a new `is_late` delivery flag by comparing estimated vs. actual delivery timestamps.
  * **Optimized Joins:** Denormalizes all four tables into one massive view, utilizing Spark `broadcast()` joins for the smaller `sellers` table to avoid expensive network shuffles.
  * **Aggregations:** Groups data by `seller_id` to generate business KPIs:
    * `total_revenue`
    * `avg_review_score`
    * `late_delivery_rate`
    * `total_orders`
  * **File Optimization:** Repartitions the final DataFrame before saving to prevent "small file syndrome" for downstream BI tools.

## 🚀 Setup & Deployment Instructions

### Prerequisites

1. **Docker Desktop** installed with **Kubernetes enabled** (Settings -> Kubernetes -> Enable Kubernetes).
2. Download the Kaggle Olist dataset and place the CSV files directly in the `data/raw_csv/` directory.


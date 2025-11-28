# Logistics ETL Pipeline 🚚

## Overview
An end-to-end data pipeline that extracts logistics data, validates it, and loads it into a Data Warehouse. Designed to simulate a real-world supply chain environment where data quality is critical.

## Architecture
**Flow:** `Python Generator (Source)` -> `Docker Container` -> `Pandas (Transformation)` -> `PostgreSQL (Warehouse)` -> `Airflow (Orchestration)`## Key Features
* **Automated Orchestration:** Scheduled daily via Apache Airflow.
* **Data Quality Gates:** Implemented SQL Check Operators to prevent 'dirty data' (NULL weights) from entering the production database.
* **Containerization:** Fully Dockerized environment using Docker Compose.
* **Fault Tolerance:** Automatic retries and alerting on pipeline failure.

## Tech Stack
* **Language:** Python 3.8
* **Orchestration:** Apache Airflow 2.7
* **Database:** PostgreSQL 13
* **Containerization:** Docker & Docker Compose

## How to Run
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/logistics-etl-pipeline.git](https://github.com/YOUR_USERNAME/logistics-etl-pipeline.git)
    ```
2.  **Start the environment:**
    ```bash
    docker compose up -d
    ```
3.  **Access Airflow:**
    * Navigate to `http://localhost:8080`
    * User: `airflow` | Password: `airflow`
4.  **Trigger the DAG:**
    * Enable `logistics_pipeline_v1` and trigger a run.




    

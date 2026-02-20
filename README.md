# Logistics ELT Pipeline with dbt 🚚
> 🚚 Production-grade ELT pipeline built with dbt Core, 
Apache Airflow, PostgreSQL & Docker. 
Simulates real-world supply chain data workflows.

## Overview
A modern **ELT (Extract, Load, Transform)** data pipeline that ingests raw logistics data, loads it into a Data Warehouse, and uses **dbt Core** for in-warehouse transformations and data quality testing. This project simulates a real-world supply chain environment where data integrity is enforced via automated schema tests.

## Architecture
**Flow:** `Python Generator (Source)` -> `PostgreSQL (Raw Layer)` -> `dbt (Transformation & Testing)` -> `PostgreSQL (Staging Layer)` -> `Airflow (Orchestration)`



[Image of ELT Architecture Diagram]


## Key Features
* **Modern ELT Architecture:** Shifted from legacy ETL to ELT, loading raw data first and transforming it inside the warehouse using **dbt**.
* **dbt Integration:** Utilizes **dbt Core** for modular SQL modeling (`stg_shipments`) and dependency management.
* **Automated Data Quality:** Implements dbt **Schema Tests** (`unique`, `not_null`, custom SQL checks) to automatically reject bad data (e.g., negative weights or duplicate IDs).
* **Containerization:** Fully Dockerized environment (Airflow + Postgres + dbt) using Docker Compose.
* **Orchestration:** Apache Airflow manages the end-to-end workflow, triggering dbt jobs via the `BashOperator`.

## Tech Stack
* **Orchestration:** Apache Airflow 2.7
* **Transformation:** dbt Core 1.8 (dbt-postgres adapter)
* **Language:** Python 3.8 & SQL
* **Database:** PostgreSQL 13
* **Containerization:** Docker & Docker Compose

## How to Run
1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Shrishsomawat/logistics-etl-pipeline.git
    ```
2.  **Start the environment:**
    ```bash
    docker compose up --build -d
    ```
3.  **Access Airflow:**
    * Navigate to `http://localhost:8080`
    * User: `airflow` | Password: `airflow`
4.  **Trigger the Pipeline:**
    * Enable the DAG named **`logistics_dbt_pipeline`**.
    * Trigger the run and watch the `dbt_run` and `dbt_test` tasks turn green!
    ---
👨‍💻 Built by [Shrish Somawat](https://github.com/Shrishsomawat) | 
Data Engineer | Amazon ROC  
🔗 [LinkedIn](https://linkedin.com/in/shrishsomawat)

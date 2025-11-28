from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator # <--- NEW IMPORT
from datetime import datetime, timedelta
from extract_logistics_data import save_data_to_file, load_data_to_postgres

default_args = {
    'owner': 'shrish',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'logistics_pipeline_v1',         
    default_args=default_args,
    description='Extract -> Load -> Quality Check',
    schedule_interval='@daily',      
    catchup=False,                   
    tags=['logistics', 'etl'],
) as dag:

    # 1. Create Table
    create_table_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS raw_shipments (
                shipment_id VARCHAR(50),
                origin_warehouse VARCHAR(50),
                destination_city VARCHAR(50),
                weight_kg FLOAT,
                status VARCHAR(50),
                timestamp VARCHAR(50)
            );
        """
    )

    # 2. Extract
    extract_task = PythonOperator(
        task_id='extract_data_daily',
        python_callable=save_data_to_file, 
    )

    # 3. Load
    load_task = PythonOperator(
        task_id='load_data_to_db',
        python_callable=load_data_to_postgres,
    )

    # 4. Quality Check (The "Gate")
    # This SQL query must return TRUE for the task to pass.
    # If it finds any row where weight_kg IS NULL, it returns FALSE (and fails).
    data_quality_check = SQLCheckOperator(
        task_id='check_for_null_weights',
        conn_id='postgres_localhost',
        sql="SELECT COUNT(*) = 0 FROM raw_shipments WHERE weight_kg IS NULL;"
    )

    # Define the flow
    create_table_task >> extract_task >> load_task >> data_quality_check
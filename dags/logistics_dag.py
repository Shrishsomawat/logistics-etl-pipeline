from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from extract_logistics_data import save_data_to_file, load_data_to_postgres

default_args = {
    'owner': 'shrish',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'logistics_dbt_pipeline',
    default_args=default_args,
    description='ELT: Extract -> Load -> dbt Transform',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'elt'],
) as dag:

    # 1. Create Table
    create_table = PostgresOperator(
        task_id='create_raw_table',
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
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=save_data_to_file,
    )

    # 3. Load
    load = PythonOperator(
        task_id='load_raw_data',
        python_callable=load_data_to_postgres,
    )

    # 4. dbt Run (TRANSFORM)
    # FIX: Added --log-path and --target-path to write to /tmp (avoids permission errors)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            '/home/airflow/.local/bin/dbt run '
            '--profiles-dir . '
            '--log-path /tmp '
            '--target-path /tmp/target'
        )
    )

    # 5. dbt Test (QUALITY)
    # FIX: Added --log-path and --target-path here too
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/dbt_project && '
            '/home/airflow/.local/bin/dbt test '
            '--profiles-dir . '
            '--log-path /tmp '
            '--target-path /tmp/target'
        )
    )

    create_table >> extract >> load >> dbt_run >> dbt_test
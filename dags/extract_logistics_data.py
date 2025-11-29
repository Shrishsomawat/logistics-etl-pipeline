import json
import random
import time
import glob
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine

# --- CONFIG ---
WAREHOUSES = ['WH-NJ-01', 'WH-TX-02', 'WH-CA-03', 'WH-IL-04']
STATUSES = ['SHIPPED', 'IN_TRANSIT', 'DELIVERED', 'DELAYED', 'LOST']
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def generate_shipment_data(num_records=50):
    data = []
    for _ in range(num_records):
        record = {
            "shipment_id": f"SHP-{random.randint(10000, 99999)}",
            "origin_warehouse": random.choice(WAREHOUSES),
            "destination_city": random.choice(['New York', 'Dallas', 'Los Angeles', 'Chicago', 'Miami']),
            "weight_kg": round(random.uniform(1.0, 50.0), 2),
            "status": random.choice(STATUSES),
            "timestamp": datetime.now().isoformat()
        }
        # Generate some dirty data (NULL weights) for dbt to clean later
        if random.random() < 0.05: 
            record['weight_kg'] = None
        data.append(record)
    return data

def save_data_to_file(**kwargs):
    shipments = generate_shipment_data()
    date_str = datetime.now().strftime("%Y-%m-%d")
    output_dir = f"/opt/airflow/data/raw/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/shipments_{int(time.time())}.json"
    
    with open(filename, 'w') as f:
        json.dump(shipments, f)
    print(f"Saved to {filename}")
    return output_dir

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    
    # FIX 1: Updated task_id to match the new DAG name
    source_dir = ti.xcom_pull(task_ids='extract_data')
    
    if not source_dir:
        raise ValueError("No directory found from previous task. Check task_ids match!")

    files = glob.glob(f"{source_dir}/*.json")
    
    all_data = []
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            all_data.extend(data)
            
    df = pd.DataFrame(all_data)
    
    # FIX 2: Removed df.dropna(). We now load dirty data (NULLs) 
    # so dbt can clean it in the next step.
    
    engine = create_engine(DB_CONN)
    df.to_sql('raw_shipments', engine, if_exists='append', index=False)
    
    print(f"Successfully loaded {len(df)} raw rows into 'raw_shipments' table.")
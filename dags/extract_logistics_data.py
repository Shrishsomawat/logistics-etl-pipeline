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
# This connection string matches what we set up in docker-compose
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
    return output_dir  # Return this path so the next task knows where to look!

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    source_dir = ti.xcom_pull(task_ids='extract_data_daily')
    
    if not source_dir:
        raise ValueError("No directory found from previous task")

    files = glob.glob(f"{source_dir}/*.json")
    
    all_data = []
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            all_data.extend(data)
            
    df = pd.DataFrame(all_data)
    
    # --- NEW: TRANSFORMATION STEP ---
    # Drop any row where weight_kg is missing
    initial_count = len(df)
    df = df.dropna(subset=['weight_kg'])
    final_count = len(df)
    print(f"Dropped {initial_count - final_count} rows with missing weights.")
    # --------------------------------
    
    engine = create_engine(DB_CONN)
    df.to_sql('raw_shipments', engine, if_exists='append', index=False)
    
    print(f"Successfully loaded {len(df)} clean rows into 'raw_shipments' table.")
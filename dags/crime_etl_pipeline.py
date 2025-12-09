from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import modul-modul kita
from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area
# Import Validator Baru
from governance.quality_checks.raw_validation import validate_raw_json_structure

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0, # Jangan retry jika validasi gagal (percuma)
}

with DAG(
    '1_ingest_lapd_crime_data',
    default_args=default_args,
    description='Enterprise Pipeline: Extract -> Validate -> Clean -> Aggregate',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lapd', 'governance']
) as dag:

    # 1. EXTRACT
    ingest_task = PythonOperator(
        task_id='1_extract_api_bronze',
        python_callable=fetch_and_upload_crime_data
    )

    # 2. VALIDATE (Satpam) - Akan gagal di sini jika data sampah
    validate_task = PythonOperator(
        task_id='2_validate_raw_quality',
        python_callable=validate_raw_json_structure
    )

    # 3. TRANSFORM (Pembersih + Schema Enforcer)
    transform_task = PythonOperator(
        task_id='3_transform_silver_governance',
        python_callable=clean_and_load_to_silver
    )

    # 4. AGGREGATE (Warehouse)
    aggregate_task = PythonOperator(
        task_id='4_load_gold_warehouse',
        python_callable=aggregate_crime_by_area
    )

    # Alur Eksekusi
    ingest_task >> validate_task >> transform_task >> aggregate_task
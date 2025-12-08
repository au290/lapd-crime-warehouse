from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import fungsi kita
from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import clean_and_load_to_silver

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    '1_ingest_lapd_crime_data',
    default_args=default_args,
    description='Pipeline End-to-End: API -> Bronze -> Silver',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['lapd', 'bronze', 'silver']
) as dag:

    # Task 1: Extract (Bronze)
    ingest_task = PythonOperator(
        task_id='ingest_api_to_bronze',
        python_callable=fetch_and_upload_crime_data
    )

    # Task 2: Transform (Silver)
    transform_task = PythonOperator(
        task_id='clean_bronze_to_silver',
        python_callable=clean_and_load_to_silver
    )

    # Mengatur Urutan: Task 1 Selesai DULU, baru Task 2 Jalan
    ingest_task >> transform_task
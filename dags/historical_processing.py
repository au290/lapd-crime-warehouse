from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import CUMA fungsi Transform & Load (Tanpa Extract)
from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
}

# DAG ini diset schedule_interval=None karena hanya dijalankan manual (Ad-hoc)
with DAG(
    '2_manual_history_processing',  # Nama DAG beda
    default_args=default_args,
    description='Pipeline Khusus: Proses data CSV Historis (Skip API)',
    schedule_interval=None,         # Tidak jalan otomatis
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['history', 'manual']
) as dag:

    # Task 1: Langsung Transform (Baca file JSON hasil upload script)
    transform_task = PythonOperator(
        task_id='process_historical_bronze',
        python_callable=clean_and_load_to_silver
    )

    # Task 2: Aggregate ke Gold
    aggregate_task = PythonOperator(
        task_id='aggregate_history_gold',
        python_callable=aggregate_crime_by_area
    )

    # Alur: Transform -> Aggregate
    transform_task >> aggregate_task
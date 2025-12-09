from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import fungsi
from src.extract.lacity_api import fetch_and_upload_crime_data
from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area
from governance.quality_checks.raw_validation import validate_raw_json_structure
# [BARU] Import Callback
from src.utils.callbacks import send_failure_alert, send_success_alert

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'retries': 0,
    # [BARU] Pasang Callback Gagal (Tag User) & Sukses (Info Biasa)
    'on_failure_callback': send_failure_alert,
    'on_success_callback': send_success_alert
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

    # Task 1: Extract
    ingest_task = PythonOperator(
        task_id='1_extract_api_bronze',
        python_callable=fetch_and_upload_crime_data
    )

    # Task 2: Validate
    validate_task = PythonOperator(
        task_id='2_validate_raw_quality',
        python_callable=validate_raw_json_structure
    )

    # Task 3: Transform
    transform_task = PythonOperator(
        task_id='3_transform_silver_governance',
        python_callable=clean_and_load_to_silver
    )

    # Task 4: Aggregate
    aggregate_task = PythonOperator(
        task_id='4_load_gold_warehouse',
        python_callable=aggregate_crime_by_area
    )

    ingest_task >> validate_task >> transform_task >> aggregate_task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import fungsi
from src.transform.fact_cleaner import clean_and_load_to_silver
from src.transform.gold_aggregator import aggregate_crime_by_area
# Import Validator (Biar sama dengan DAG harian)
from governance.quality_checks.raw_validation import validate_raw_json_structure

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
}

with DAG(
    '2_manual_history_processing',
    default_args=default_args,
    description='Pipeline Khusus: Validate -> Clean -> Aggregate (CSV Historis)',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['history', 'manual', 'governance']
) as dag:

    # Task 1: Validation (Satpam)
    validate_task = PythonOperator(
        task_id='validate_historical_data',
        python_callable=validate_raw_json_structure
    )

    # Task 2: Transform (Pembersih)
    transform_task = PythonOperator(
        task_id='process_historical_bronze',
        python_callable=clean_and_load_to_silver
    )

    # Task 3: Aggregate (Gold)
    aggregate_task = PythonOperator(
        task_id='aggregate_history_gold',
        python_callable=aggregate_crime_by_area
    )

    # Alur: Validate -> Transform -> Aggregate
    validate_task >> transform_task >> aggregate_task
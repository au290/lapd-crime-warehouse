from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from src.machine_learning.train_prophet import train_and_save_model

default_args = {
    'owner': 'data-scientist',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    '3_train_forecasting_model',
    default_args=default_args,
    description='Melatih ulang model Prophet setiap hari',
    schedule_interval='0 6 * * *', # Jalan jam 6 pagi (setelah ETL subuh)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['machine-learning', 'prophet']
) as dag:

    train_task = PythonOperator(
        task_id='train_prophet_model',
        python_callable=train_and_save_model
    )

    train_task
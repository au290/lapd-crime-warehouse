from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# [FIX] Import fungsi yang BARU (send_failure_alert), bukan yang lama
from src.utils.callbacks import send_failure_alert

def sengaja_error():
    print("Mencoba menjalankan tugas...")
    raise ValueError("Ups! Ini adalah simulasi error untuk mengetes Alarm Discord.")

default_args = {
    'owner': 'tester',
    'depends_on_past': False,
    # [FIX] Gunakan fungsi callback yang baru
    'on_failure_callback': send_failure_alert  
}

with DAG(
    '00_test_alert_system',
    default_args=default_args,
    description='DAG ini PASTI GAGAL untuk tes Webhook',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test']
) as dag:

    fail_task = PythonOperator(
        task_id='task_pemicu_badai',
        python_callable=sengaja_error
    )
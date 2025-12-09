from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Setup Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import callback
from src.utils.callbacks import send_success_alert


def tugas_sukses():
    print("Tugas berjalan lancar tanpa error!")
    return "Sukses bos!"

default_args = {
    'owner': 'tester',
    'depends_on_past': False,
    'on_success_callback': send_success_alert  # <-- callback sukses
}

with DAG(
    '00_test_alert_success',
    default_args=default_args,
    description='DAG ini PASTI SUKSES untuk tes Webhook Success',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test']
) as dag:

    success_task = PythonOperator(
        task_id='task_yang_pasti_sukses',
        python_callable=tugas_sukses
    )

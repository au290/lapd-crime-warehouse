from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Setup path agar bisa import script dari folder scripts/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

def trigger_backup_wrapper():
    # Import di dalam fungsi untuk menghindari error path saat Airflow parsing awal
    import sys
    sys.path.append('/opt/airflow/scripts')
    from backup_minio import perform_backup
    perform_backup()

default_args = {
    'owner': 'infrastructure',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '99_disaster_recovery_backup',
    default_args=default_args,
    description='Melakukan Snapshot Backup harian ke Archive Bucket',
    schedule_interval='0 23 * * *', # Jalan tiap jam 23:00 malam
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['disaster-recovery', 'maintenance']
) as dag:

    backup_task = PythonOperator(
        task_id='execute_minio_snapshot',
        python_callable=trigger_backup_wrapper
    )

    backup_task
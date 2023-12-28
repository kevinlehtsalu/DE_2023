from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from json_to_postgres import main as import_json_to_postgres

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 28),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'json_to_postgres_import',
    default_args=default_args,
    description='DAG for importing JSON data to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

import_json_task = PythonOperator(
    task_id='import_json_to_postgres',
    python_callable=import_json_to_postgres,
    dag=dag,
)

import_json_task

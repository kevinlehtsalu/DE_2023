from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import statement

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_data_processing_dag',
    default_args=default_args,
    description='A simple data processing dags',
    schedule=timedelta(days=1)  # Updated parameter
)
def load_initial_data():
    # Your code to load data into PostgreSQL
    pass

def enrich_data():
    # Your data enrichment code
    pass

def export_data():
    # Your data export code
    pass

def import_data():
    # Your data import code for Neo4j
    pass

load_task = PythonOperator(
    task_id='load_initial_data',
    python_callable=load_initial_data,
    dag=dag,
)

enrich_task = PythonOperator(
    task_id='enrich_data',
    python_callable=enrich_data,
    dag=dag,
)

export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data,
    dag=dag,
)

import_task = PythonOperator(
    task_id='import_data',
    python_callable=import_data,
    dag=dag,
)

load_task >> enrich_task >> export_task >> import_task

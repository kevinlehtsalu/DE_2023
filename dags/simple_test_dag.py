from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json

def read_json_file():
    file_path = '/opt/airflow/data/data.json'  # Replace with the path to your JSON file
    with open(file_path, 'r') as file:
        data = json.load(file)
        print(data)  # This will print the data to the logs


def my_test_function():
    print("This is a test message to check the DAG location.")

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
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='print_test_message',
    python_callable=my_test_function,
    dag=dag,
)

read_json_task = PythonOperator(
    task_id='read_json_file',
    python_callable=read_json_file,
    dag=dag,
)

# Set the order of execution if necessary
t1 >> read_json_task
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from datetime import datetime, timedelta
#
# # These args will get passed on to the bash command
# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2023, 12, 29),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=1),
#     'catchup': False,
# }
#
# dag = DAG(
#     'export_postgres_data',
#     default_args=default_args,
#     description='DAG for exporting data from PostgreSQL to a shared volume',
#     schedule_interval=timedelta(days=1),  # Change to your preferred interval
# )
#
# # Define the command to execute the export script within the PostgreSQL container
# export_command = (
#     "docker exec arxiv_postgres python3 /shared_data/export.py; echo $?"
# )
#
#
# # Create a BashOperator to run the export command
# export_data_task = BashOperator(
#     task_id='export_postgres_data',
#     bash_command=export_command,
#     dag=dag,
# )
#
# export_data_task

##############################################################################################################
#
# import json
# import psycopg2
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
#
# def export_data_to_json():
#     # Database connection parameters
#     db_params = {
#         'dbname': 'arxiv',
#         'user': 'admin',
#         'password': 'admin',
#         'host': 'postgres',  # Use the service name from your Docker Compose
#         'port': '5432'
#     }
#
#     # Connect to the PostgreSQL database
#     conn = psycopg2.connect(**db_params)
#     cur = conn.cursor()
#
#     # Shared storage path
#     shared_path = '/opt/airflow/data'  # Replace with the actual path where your shared volume is mounted
#
#     # Function to export nodes to JSON
#     def export_nodes_to_json(table_name):
#         query = f"SELECT row_to_json(t) FROM (SELECT * FROM {table_name}) t"
#         cur.execute(query)
#         rows = cur.fetchall()
#         data = [row[0] for row in rows]
#
#         with open(f'{shared_path}/{table_name}_nodes.json', 'w', encoding='utf-8') as jsonfile:
#             json.dump(data, jsonfile, ensure_ascii=False, indent=4)
#
#     # Function to export relationships to JSON
#     def export_relationships_to_json(relationship_table, start_table, end_table, start_field, end_field):
#         query = f"""
#         SELECT json_agg(t) AS data
#         FROM (
#             SELECT p.{start_field} AS start_id, a.{end_field} AS end_id
#             FROM {relationship_table}
#             INNER JOIN {start_table} p ON {relationship_table}.{start_field} = p.{start_field}
#             INNER JOIN {end_table} a ON {relationship_table}.{end_field} = a.{end_field}
#         ) t
#         """
#         cur.execute(query)
#         result = cur.fetchone()
#         data = result[0] if result[0] is not None else []
#
#         with open(f'{shared_path}/{relationship_table}_relationships.json', 'w', encoding='utf-8') as jsonfile:
#             json.dump(data, jsonfile, ensure_ascii=False, indent=4)
#
#     # Export nodes
#     for table in ['authors', 'categories', 'versions', 'papers']:
#         export_nodes_to_json(table)
#
#     # Export relationships
#     export_relationships_to_json('paper_authors', 'papers', 'authors', 'paper_id', 'author_id')
#     export_relationships_to_json('paper_categories', 'papers', 'categories', 'paper_id', 'category_id')
#
#     # Close the database connection
#     cur.close()
#     conn.close()
#
# # Define the DAG
# dag = DAG(
#     'export_postgres_data_to_json',
#     default_args={
#         'owner': 'airflow',
#         'start_date': datetime(2023, 12, 29),
#         'retries': 1,
#         'retry_delay': timedelta(minutes=1),
#     },
#     description='DAG to export data from PostgreSQL to JSON',
#     schedule_interval=timedelta(days=1),
# )
#
# # Define the task using PythonOperator
# export_task = PythonOperator(
#     task_id='export_data',
#     python_callable=export_data_to_json,
#     dag=dag,
# )
#
# export_task
##############################################################################################################
import json
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

def export_data_to_json(ds, **kwargs):
    # Define the database connection
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # Function to export nodes to JSON
    def export_nodes_to_json(table_name, cur):
        query = f"SELECT row_to_json(t) FROM (SELECT * FROM {table_name}) t"
        cur.execute(query)
        rows = cur.fetchall()
        data = [row[0] for row in rows]

        # Write to a file within the Airflow worker's accessible directory
        with open(f'/opt/airflow/data/{table_name}_nodes.json', 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, ensure_ascii=False, indent=4)

    # Function to export relationships to JSON
    def export_relationships_to_json(relationship_table, start_table, end_table, start_field, end_field, cur):
        query = f"""
        SELECT json_agg(t) AS data
        FROM (
            SELECT p.{start_field} AS start_id, a.{end_field} AS end_id
            FROM {relationship_table}
            INNER JOIN {start_table} p ON {relationship_table}.{start_field} = p.{start_field}
            INNER JOIN {end_table} a ON {relationship_table}.{end_field} = a.{end_field}
        ) t
        """
        cur.execute(query)
        result = cur.fetchone()
        data = result[0] if result[0] is not None else []

        with open(f'/opt/airflow/data/{relationship_table}_relationships.json', 'w', encoding='utf-8') as jsonfile:
            json.dump(data, jsonfile, ensure_ascii=False, indent=4)

    # Export nodes and relationships
    for table in ['authors', 'categories', 'versions', 'papers']:
        export_nodes_to_json(table, cur)

    export_relationships_to_json('paper_authors', 'papers', 'authors', 'paper_id', 'author_id', cur)
    export_relationships_to_json('paper_categories', 'papers', 'categories', 'paper_id', 'category_id', cur)

    # Close the database connection
    cur.close()
    conn.close()

# Define the DAG
dag = DAG(
    'export_postgres_data_to_json',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 12, 29),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='DAG to export data from PostgreSQL to JSON',
    schedule_interval=timedelta(days=1),
)

# Define the task using PythonOperator
export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data_to_json,
    provide_context=True,
    dag=dag,
)

export_task



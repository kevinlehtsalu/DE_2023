import json
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from neo4j import GraphDatabase

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'to_neo4j',
    default_args=default_args,
    description='DAG to export data from PostgreSQL to JSON and send to Neo4J',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

dag_run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

def export_table_to_json(table_name, cur, export_path, timestamp):
    query = f"SELECT row_to_json(t) FROM (SELECT * FROM {table_name}) t"
    cur.execute(query)
    rows = cur.fetchall()
    data = [row[0] for row in rows]
    
    # Use a context manager to handle file writing
    file_path = os.path.join(export_path, f"{table_name}_nodes_{timestamp}.json")
    with open(file_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)
        print(f"Exported {table_name} to {file_path}")

def export_relationships_to_json(relationship_table, start_table, end_table, start_field, end_field, cur, export_path, timestamp):
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

    # Use a context manager to handle file writing
    file_path = f'{export_path}/{relationship_table}_relationships_{timestamp}.json'
    with open(file_path, 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)

def export_data_to_json(timestamp, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get the path for exporting JSON files from Airflow variables
    export_path = "/opt/airflow/shared"
    
    # Ensure the export path directory exists
    os.makedirs(export_path, exist_ok=True)

    # Create a connection to the database using the hook
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Call the export functions for each table
            for table in ['authors', 'categories', 'versions', 'papers']:
                export_table_to_json(table, cur, export_path, timestamp)
            
            # Call the export functions for relationships
            export_relationships_to_json('paper_authors', 'papers', 'authors', 'paper_id', 'author_id', cur, export_path, timestamp)
            export_relationships_to_json('paper_categories', 'papers', 'categories', 'paper_id', 'category_id', cur, export_path, timestamp)

# This function will create a Neo4j session and import the JSON data
def import_data_to_neo4j(timestamp):
    uri = "neo4j://my_neo4j:7687"  # Replace with your Neo4j URI
    username = "neo4j"             # Replace with your Neo4j username
    password = "123456789"         # Replace with your Neo4j password

    # Connect to Neo4j using the driver
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        # Define the paths to the JSON files
        node_files = {
            'authors': 'Author',
            'categories': 'Category',
            'papers': 'Paper',
            'versions': 'Version'
        }
        rel_files = {
            'paper_authors': ('WRITTEN_BY', 'Paper', 'paper_id', 'Author', 'author_id'),
            'paper_categories': ('BELONGS_TO', 'Paper', 'paper_id', 'Category', 'category_id')
        }

        # Import nodes
        for json_filename, label in node_files.items():
            file_path = f'file:///{json_filename}_nodes_{timestamp}.json'  # Airflow's path to the shared directory
            create_nodes_query = f"""
            CALL apoc.load.json($file_path) YIELD value AS data
            UNWIND data AS row
            MERGE (n:{label} {{{'id'}: row.id}})
            ON CREATE SET n += row
            ON MATCH SET n += row
            """

            session.run(create_nodes_query, parameters={'file_path': file_path, 'label': label, 'primary_key_field': 'id'})

        # Import relationships
        for json_filename, (relationship_type, start_label, start_key, end_label, end_key) in rel_files.items():
            file_path = f'file:///{json_filename}_relationships_{timestamp}.json'  # Airflow's path to the shared directory
            create_rels_query = """
            CALL apoc.load.json($file_path) YIELD value AS data
            UNWIND data AS rel
            MATCH (start:{start_label} {{ {start_key}: rel.start_id }})
            MATCH (end:{end_label} {{ {end_key}: rel.end_id }})
            MERGE (start)-[r:{relationship_type}]->(end)
            """
            session.run(create_rels_query, parameters={'file_path': file_path, 'relationship_type': relationship_type, 'start_label': start_label, 'start_key': start_key, 'end_label': end_label, 'end_key': end_key})

        print("JSON data import completed.")

    # Close the driver connection
    driver.close()


export_task = PythonOperator(
    task_id='export_data',
    python_callable=export_data_to_json,
    op_kwargs={'timestamp': dag_run_timestamp},
    dag=dag,
)

import_to_neo4j_task = PythonOperator(
    task_id='import_to_neo4j',
    python_callable=import_data_to_neo4j,
    op_kwargs={'timestamp': dag_run_timestamp},
    dag=dag,
)

export_task >> import_to_neo4j_task

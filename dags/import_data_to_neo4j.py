from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# These are the DAG's default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# This is the DAG definition
dag = DAG(
    'import_data_to_neo4j',
    default_args=default_args,
    description='Import JSON data into Neo4j',
    schedule_interval=None,  # Manual trigger
    catchup=False
)

# This function will create a Neo4j session and import the JSON data
def import_data_to_neo4j():
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
            file_path = f'file:///opt/airflow/shared/{json_filename}_nodes.json'  # Airflow's path to the shared directory
            create_nodes_query = """
            CALL apoc.load.json($file_path) YIELD value AS data
            UNWIND data AS row
            MERGE (n:{label} {{ {primary_key_field}: row.{primary_key_field} }})
            ON CREATE SET n += row
            ON MATCH SET n += row
            """
            session.run(create_nodes_query, parameters={'file_path': file_path, 'label': label, 'primary_key_field': 'id'})

        # Import relationships
        for json_filename, (relationship_type, start_label, start_key, end_label, end_key) in rel_files.items():
            file_path = f'file:///opt/airflow/shared/{json_filename}_relationships.json'  # Airflow's path to the shared directory
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

# Define the PythonOperator to execute the import_data_to_neo4j function
import_to_neo4j_task = PythonOperator(
    task_id='import_to_neo4j',
    python_callable=import_data_to_neo4j,
    dag=dag
)

# The task is a standalone task with no dependencies
import_to_neo4j_task

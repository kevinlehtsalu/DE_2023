from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from neo4j import GraphDatabase

# Neo4j connection parameters
NEO4J_CONN_URI = "bolt://my_neo4j:7687"  # Use 'bolt' protocol
NEO4J_USERNAME = "neo4j"
NEO4J_PASSWORD = "123456789"

# Functions for Neo4j data import
def import_nodes_to_neo4j(json_filename, label, primary_key_field):
    driver = GraphDatabase.driver(NEO4J_CONN_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    with driver.session() as session:
        # Adjust the file path to match the Neo4j import directory
        file_path = f'file:///import/{json_filename}_nodes.json'
        create_nodes_query = f"""
        CALL apoc.load.json($file_path) YIELD value AS data
        UNWIND data AS row
        MERGE (n:{label} {{{primary_key_field}: row.{primary_key_field}}})
        ON CREATE SET n += row
        ON MATCH SET n += row
        """
        session.run(create_nodes_query, parameters={'file_path': file_path})
    driver.close()

def create_relationships_in_neo4j(json_filename, relationship_type, start_label, start_key, end_label, end_key):
    driver = GraphDatabase.driver(NEO4J_CONN_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
    with driver.session() as session:
        # Adjust the file path to match the Neo4j import directory
        file_path = f'file:///import/{json_filename}_relationships.json'
        create_rels_query = f"""
        CALL apoc.load.json($file_path) YIELD value AS data
        UNWIND data AS rel
        MATCH (start:{start_label} {{{start_key}: rel.start_id}})
        MATCH (end:{end_label} {{{end_key}: rel.end_id}})
        MERGE (start)-[r:{relationship_type}]->(end)
        """
        session.run(create_rels_query, parameters={'file_path': file_path})
    driver.close()

# Define the DAG
dag = DAG(
    'neo4j_import_data',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 12, 29),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG to import JSON data into Neo4j',
    schedule_interval=None,  # Manual trigger
    catchup=False,
)

# Tasks to import nodes into Neo4j
import_authors_task = PythonOperator(
    task_id='import_authors',
    python_callable=import_nodes_to_neo4j,
    op_args=['authors', 'Author', 'author_id'],
    dag=dag,
)

import_categories_task = PythonOperator(
    task_id='import_categories',
    python_callable=import_nodes_to_neo4j,
    op_args=['categories', 'Category', 'category_id'],
    dag=dag,
)

import_papers_task = PythonOperator(
    task_id='import_papers',
    python_callable=import_nodes_to_neo4j,
    op_args=['papers', 'Paper', 'paper_id'],
    dag=dag,
)

import_versions_task = PythonOperator(
    task_id='import_versions',
    python_callable=import_nodes_to_neo4j,
    op_args=['versions', 'Version', 'version_id'],
    dag=dag,
)

# Tasks to create relationships in Neo4j
create_paper_authors_relationship_task = PythonOperator(
    task_id='create_paper_authors_relationship',
    python_callable=create_relationships_in_neo4j,
    op_args=['paper_authors', 'WRITTEN_BY', 'Paper', 'paper_id', 'Author', 'author_id'],
    dag=dag,
)

create_paper_categories_relationship_task = PythonOperator(
    task_id='create_paper_categories_relationship',
    python_callable=create_relationships_in_neo4j,
    op_args=['paper_categories', 'BELONGS_TO', 'Paper', 'paper_id', 'Category', 'category_id'],
    dag=dag,
)

# Define dependencies: Nodes must be imported before relationships can be created
import_authors_task >> create_paper_authors_relationship_task
import_categories_task >> create_paper_categories_relationship_task
import_papers_task >> [create_paper_authors_relationship_task, create_paper_categories_relationship_task]
import_versions_task  # No dependency for versions as they don't have a relationship task

if __name__ == "__main__":
    dag.cli()

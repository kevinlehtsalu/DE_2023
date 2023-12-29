# # from neo4j import GraphDatabase
# #
# # uri = "neo4j://localhost:7687"
# # username = "neo4j"
# # password = "123456789"
# #
# # driver = GraphDatabase.driver(uri, auth=(username, password))
# #
# # def import_data(session):
# #     # Import Authors
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row
# #         MERGE (a:Author {author_id: toInteger(row.author_id)})
# #         ON CREATE SET a.last_name = row.last_name,
# #                        a.first_name = row.first_name,
# #                        a.middle_name = row.middle_name
# #         """)
# #     except Exception as e:
# #         print(f"Error importing authors: {e}")
# #
# #     # Import Categories
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
# #         MERGE (c:Category {category_id: toInteger(row.category_id)})
# #         ON CREATE SET c.category_name = row.category_name
# #         """)
# #     except Exception as e:
# #         print(f"Error importing categories: {e}")
# #
# #     # Import Papers
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
# #         MERGE (p:Paper {paper_id: row.paper_id})
# #         ON CREATE SET p.title = row.title,
# #                        p.submitter = row.submitter,
# #                        p.comments = row.comments,
# #                        p.journal_ref = row.journal_ref,
# #                        p.doi = row.doi,
# #                        p.report_no = row.report_no,
# #                        p.abstract = row.abstract,
# #                        p.update_date = row.update_date,
# #                        p.citation_count = toInteger(row.citation_count)
# #         """)
# #     except Exception as e:
# #         print(f"Error importing papers: {e}")
# #
# #     # Import Versions
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///versions.csv' AS row
# #         MERGE (v:Version {version_id: toInteger(row.version_id)})
# #         ON CREATE SET v.version = row.version,
# #                        v.created_date = row.created_date
# #         """)
# #     except Exception as e:
# #         print(f"Error importing versions: {e}")
# #
# #     # Create Paper-Author Relationships
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///paper_authors.csv' AS row
# #         MATCH (p:Paper {paper_id: row.paper_id})
# #         MATCH (a:Author {author_id: toInteger(row.author_id)})
# #         MERGE (p)-[:WRITTEN_BY]->(a)
# #         """)
# #     except Exception as e:
# #         print(f"Error creating paper-author relationships: {e}")
# #
# #     # Create Paper-Category Relationships
# #     try:
# #         session.run("""
# #         LOAD CSV WITH HEADERS FROM 'file:///paper_categories.csv' AS row
# #         MATCH (p:Paper {paper_id: row.paper_id})
# #         MATCH (c:Category {category_id: toInteger(row.category_id)})
# #         MERGE (p)-[:BELONGS_TO]->(c)
# #         """)
# #     except Exception as e:
# #         print(f"Error creating paper-category relationships: {e}")
# #
# # # Running the import
# # with driver.session() as session:
# #     print("Starting data import...")
# #     import_data(session)
# #     print("Data import completed.")
# #
# # driver.close()
#
# #####################################################################################################
#
# from neo4j import GraphDatabase
#
# # Neo4j connection URI and credentials
# uri = "neo4j://localhost:7687"
# username = "neo4j"
# password = "123456789"
#
# # Connect to Neo4j using the driver
# driver = GraphDatabase.driver(uri, auth=(username, password))
#
#
# def import_json_data(session, json_filename, label, primary_key_field):
#     file_path = f'file:///{json_filename}_nodes.json'
#
#     # Cypher query to load JSON data and create nodes
#     create_nodes_query = f"""
#     CALL apoc.load.json($file_path) YIELD value AS data
#     UNWIND data AS row
#     MERGE (n:{label} {{{primary_key_field}: row.{primary_key_field}}})
#     ON CREATE SET n += row
#     ON MATCH SET n += row
#     """
#     session.run(create_nodes_query, file_path=file_path)
#
#
# def create_relationships(session, json_filename, relationship_type, start_label, start_key, end_label, end_key):
#     file_path = f'file:///{json_filename}_relationships.json'
#
#     # Cypher query to load JSON data and create relationships
#     create_rels_query = f"""
#     CALL apoc.load.json($file_path) YIELD value AS data
#     UNWIND data AS rel
#     MATCH (start:{start_label} {{{start_key}: rel.start_id}})
#     MATCH (end:{end_label} {{{end_key}: rel.end_id}})
#     MERGE (start)-[r:{relationship_type}]->(end)
#     """
#     session.run(create_rels_query, file_path=file_path)
#
#
#
# # Execute the import within a Neo4j session
# with driver.session() as session:
#     print("Starting JSON data import...")
#
#     # Import data for nodes
#     import_json_data(session, 'authors', 'Author', 'author_id')
#     import_json_data(session, 'categories', 'Category', 'category_id')
#     import_json_data(session, 'papers', 'Paper', 'paper_id')
#     import_json_data(session, 'versions', 'Version', 'version_id')
#
#     # Create relationships
#     create_relationships(session, 'paper_authors', 'WRITTEN_BY', 'Paper', 'paper_id', 'Author', 'author_id')
#     create_relationships(session, 'paper_categories', 'BELONGS_TO', 'Paper', 'paper_id', 'Category', 'category_id')
#
#     print("JSON data import completed.")
#
# # Close the driver connection
# driver.close()
#######################################################################################################################
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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
    uri = "neo4j://my_neo4j:7687"  # Use the container name as the hostname
    username = "neo4j"
    password = "123456789"

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
            file_path = f'/var/lib/neo4j/import/{json_filename}_nodes.json'  # Path inside the Neo4j container
            create_nodes_query = """
            CALL apoc.load.json($file_path) YIELD value AS data
            UNWIND data AS row
            MERGE (n:{label} {{ {primary_key_field}: row.{primary_key_field} }})
            ON CREATE SET n += row
            ON MATCH SET n += row
            """
            session.run(create_nodes_query, parameters={'file_path': file_path, 'label': label, 'primary_key_field': 'id'})

        # Create relationships
        for json_filename, (relationship_type, start_label, start_key, end_label, end_key) in rel_files.items():
            file_path = f'/var/lib/neo4j/import/{json_filename}_relationships.json'  # Path inside the Neo4j container
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

# Set the task dependencies
import_to_neo4j_task




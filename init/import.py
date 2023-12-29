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


from neo4j import GraphDatabase

# Neo4j connection URI and credentials
uri = "neo4j://localhost:7687"
username = "neo4j"
password = "123456789"

# Connect to Neo4j using the driver
driver = GraphDatabase.driver(uri, auth=(username, password))

def import_json_data(session, json_filename, label, primary_key_field):
    file_path = f'file:///import/{json_filename}_nodes.json'  # Updated file path

    # Cypher query to load JSON data and create nodes
    create_nodes_query = f"""
    CALL apoc.load.json($file_path) YIELD value AS data
    UNWIND data AS row
    MERGE (n:{label} {{{primary_key_field}: row.{primary_key_field}}})
    ON CREATE SET n += row
    ON MATCH SET n += row
    """
    session.run(create_nodes_query, file_path=file_path)

def create_relationships(session, json_filename, relationship_type, start_label, start_key, end_label, end_key):
    file_path = f'file:///import/{json_filename}_relationships.json'  # Updated file path

    # Cypher query to load JSON data and create relationships
    create_rels_query = f"""
    CALL apoc.load.json($file_path) YIELD value AS data
    UNWIND data AS rel
    MATCH (start:{start_label} {{{start_key}: rel.start_id}})
    MATCH (end:{end_label} {{{end_key}: rel.end_id}})
    MERGE (start)-[r:{relationship_type}]->(end)
    """
    session.run(create_rels_query, file_path=file_path)

# Execute the import within a Neo4j session
with driver.session() as session:
    print("Starting JSON data import...")

    # Import data for nodes
    import_json_data(session, 'authors', 'Author', 'author_id')
    import_json_data(session, 'categories', 'Category', 'category_id')
    import_json_data(session, 'papers', 'Paper', 'paper_id')
    import_json_data(session, 'versions', 'Version', 'version_id')

    # Create relationships
    create_relationships(session, 'paper_authors', 'WRITTEN_BY', 'Paper', 'paper_id', 'Author', 'author_id')
    create_relationships(session, 'paper_categories', 'BELONGS_TO', 'Paper', 'paper_id', 'Category', 'category_id')

    print("JSON data import completed.")

# Close the driver connection
driver.close()



# # import psycopg2
# # import csv
# #
# # # Database connection parameters
# # db_params = {
# #     'dbname': 'arxiv',
# #     'user': 'admin',
# #     'password': 'admin',
# #     'host': 'localhost'
# # }
# #
# # # Connect to the PostgreSQL database
# # conn = psycopg2.connect(**db_params)
# # cur = conn.cursor()
# #
# # # Function to export a single table
# # def export_table(table_name):
# #     query = f"SELECT * FROM {table_name}"
# #     with open(f'{table_name}.csv', 'w', newline='', encoding='utf-8') as csvfile:
# #         csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)  # Quote all fields
# #         cur.execute(query)
# #         # Write the header
# #         csvwriter.writerow([desc[0] for desc in cur.description])
# #         # Write data rows
# #         for row in cur:
# #             formatted_row = [str(field).replace('"', '""') if field is not None else '' for field in row]
# #             csvwriter.writerow(formatted_row)
# #
# # # Export each table
# # for table in ['authors', 'categories', 'versions', 'papers', 'paper_authors', 'paper_categories']:
# #     export_table(table)
# #
# # # Close the database connection
# # cur.close()
# # conn.close()
# #############################################################################################################
# import psycopg2
# import json
#
# # Database connection parameters
# db_params = {
#     'dbname': 'arxiv',
#     'user': 'admin',
#     'password': 'admin',
#     'host': 'localhost'
# }
#
# # Connect to the PostgreSQL database
# conn = psycopg2.connect(**db_params)
# cur = conn.cursor()
#
# # Function to export nodes to JSON
# def export_nodes_to_json(table_name):
#     query = f"SELECT row_to_json(t) FROM (SELECT * FROM {table_name}) t"
#     cur.execute(query)
#     rows = cur.fetchall()
#     data = [row[0] for row in rows]
#
#     with open(f'{table_name}_nodes.json', 'w', encoding='utf-8') as jsonfile:
#         json.dump(data, jsonfile, ensure_ascii=False, indent=4)
#
# # Function to export relationships to JSON
# def export_relationships_to_json(relationship_table, start_table, end_table, start_field, end_field):
#     query = f"""
#     SELECT json_agg(t) AS data
#     FROM (
#         SELECT p.{start_field} AS start_id, a.{end_field} AS end_id
#         FROM {relationship_table}
#         INNER JOIN {start_table} p ON {relationship_table}.{start_field} = p.{start_field}
#         INNER JOIN {end_table} a ON {relationship_table}.{end_field} = a.{end_field}
#     ) t
#     """
#     cur.execute(query)
#     result = cur.fetchone()
#     data = result[0] if result[0] is not None else []
#
#     with open(f'{relationship_table}_relationships.json', 'w', encoding='utf-8') as jsonfile:
#         json.dump(data, jsonfile, ensure_ascii=False, indent=4)
#
# # Export nodes
# export_nodes_to_json('authors')
# export_nodes_to_json('categories')
# export_nodes_to_json('versions')
# export_nodes_to_json('papers')
#
# # Export relationships
# export_relationships_to_json('paper_authors', 'papers', 'authors', 'paper_id', 'author_id')
# export_relationships_to_json('paper_categories', 'papers', 'categories', 'paper_id', 'category_id')
#
# # Close the database connection
# cur.close()
# conn.close()
#
#

#########################################################################################################

import psycopg2
import json

# Database connection parameters
db_params = {
    'dbname': 'arxiv',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost'
}

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Function to export nodes to JSON
def export_nodes_to_json(table_name, shared_path):
    query = f"SELECT row_to_json(t) FROM (SELECT * FROM {table_name}) t"
    cur.execute(query)
    rows = cur.fetchall()
    data = [row[0] for row in rows]

    with open(f'{shared_path}/{table_name}_nodes.json', 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)

# Function to export relationships to JSON
def export_relationships_to_json(relationship_table, start_table, end_table, start_field, end_field, shared_path):
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

    with open(f'{shared_path}/{relationship_table}_relationships.json', 'w', encoding='utf-8') as jsonfile:
        json.dump(data, jsonfile, ensure_ascii=False, indent=4)

# Shared storage path
shared_path = '/shared_data'

# Export nodes
export_nodes_to_json('authors', shared_path)
export_nodes_to_json('categories', shared_path)
export_nodes_to_json('versions', shared_path)
export_nodes_to_json('papers', shared_path)

# Export relationships
export_relationships_to_json('paper_authors', 'papers', 'authors', 'paper_id', 'author_id', shared_path)
export_relationships_to_json('paper_categories', 'papers', 'categories', 'paper_id', 'category_id', shared_path)

# Close the database connection
cur.close()
conn.close()

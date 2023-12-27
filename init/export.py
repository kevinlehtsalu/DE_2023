import psycopg2
import csv

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

# Function to export a single table
def export_table(table_name):
    query = f"SELECT * FROM {table_name}"
    with open(f'{table_name}.csv', 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)  # Quote all fields
        cur.execute(query)
        # Write the header
        csvwriter.writerow([desc[0] for desc in cur.description])
        # Write data rows
        for row in cur:
            formatted_row = [str(field).replace('"', '""') if field is not None else '' for field in row]
            csvwriter.writerow(formatted_row)

# Export each table
for table in ['authors', 'categories', 'versions', 'papers', 'paper_authors', 'paper_categories']:
    export_table(table)

# Close the database connection
cur.close()
conn.close()

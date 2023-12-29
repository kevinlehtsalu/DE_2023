import json
import psycopg2
from psycopg2.extras import execute_values
#from airflow.models import Variable

# Function to insert authors and return their IDs
def insert_authors(cursor, authors_parsed):
    author_ids = []
    for author in authors_parsed:
        author_tuple = tuple(author + [""] * (3 - len(author)))
        try:
            cursor.execute(
                """
                INSERT INTO authors (last_name, first_name, middle_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (last_name, first_name, middle_name)
                DO UPDATE SET last_name = EXCLUDED.last_name
                RETURNING author_id;
                """,
                author_tuple
            )
            author_id = cursor.fetchone()[0]
            author_ids.append(author_id)
        except psycopg2.Error as e:
            print(f"Database error occurred while inserting authors: {e}")
            conn.rollback()
    return author_ids


# Function to insert categories and return their IDs
def insert_categories(cursor, categories):
    category_ids = []
    for category in categories:
        if category:  # Ensure category is not empty
            try:
                cursor.execute(
                    """
                    INSERT INTO categories (category_name)
                    VALUES (%s)
                    ON CONFLICT (category_name)
                    DO NOTHING
                    RETURNING category_id;
                    """,
                    (category,)
                )
                result = cursor.fetchone()
                category_id = result[0] if result else None
                if category_id:
                    category_ids.append(category_id)
            except psycopg2.Error as e:
                print(f"Database error occurred while inserting categories: {e}")
                conn.rollback()
    return category_ids


# Function to load data from JSON record
def load_data(cursor, record):
    try:
        authors_parsed = record.get('authors_parsed', [])
        categories = record.get('categories', '').split()
        author_ids = insert_authors(cursor, authors_parsed)
        category_ids = insert_categories(cursor, categories)

        paper_id = record['id']
        cursor.execute(
            """
            INSERT INTO papers (paper_id, title, submitter, comments, journal_ref, doi, report_no, abstract, update_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (paper_id) DO NOTHING;
            """,
            (paper_id, record.get('title'), record.get('submitter'), record.get('comments'),
             record.get('journal-ref'), record.get('doi'), record.get('report-no'),
             record.get('abstract'), record.get('update_date'))
        )

        for author_id in author_ids:
            cursor.execute(
                "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (paper_id, author_id)
            )

        for category_id in category_ids:
            cursor.execute(
                "INSERT INTO paper_categories (paper_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (paper_id, category_id)
            )
    except psycopg2.Error as e:
        print(f"Database error occurred: {e}")
        conn.rollback()


# Connect to the PostgreSQL database
try:
    conn = psycopg2.connect(
        dbname='arxiv',
        user='admin',
        password='admin',
        host='localhost'
    )
    conn.autocommit = True
    cursor = conn.cursor()
except psycopg2.Error as e:
    print(f"Error connecting to the database: {e}")
    exit(1)

# Read and process the JSON file
try:
    with open('archive/data.json', 'r',
              encoding='utf-8') as file:
        for line in file:
            try:
                record = json.loads(line)
                load_data(cursor, record)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON line: {e}")
            except Exception as e:
                print(f"An error occurred: {e}")
except Exception as e:
    print(f"Error reading the file: {e}")
# Close the connection

cursor.close()
conn.close()

import os
import json
import psycopg2
import logging
from contextlib import contextmanager
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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



@contextmanager
def get_db_cursor():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()

def process_file(file_path):
    with get_db_cursor() as cursor:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    try:
                        record = json.loads(line)
                        load_data(cursor, record)
                    except json.JSONDecodeError as e:
                        logger.error(f"Failed to parse JSON line: {e}")
                    except Exception as e:
                        logger.error(f"An error occurred: {e}")
        except Exception as e:
            logger.error(f"Error reading the file: {e}")

def main():
    # Fetch the file path from Airflow Variables
    file_path = Variable.get("json_file_path", default_var="/path/to/your/data.json")
    process_file(file_path)

if __name__ == "__main__":
    main()

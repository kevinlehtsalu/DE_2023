from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from psycopg2.extras import execute_values


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

def process_record(cursor, record):
    # Extract author data and insert, returning IDs
    authors_parsed = record.get('authors_parsed', [])
    author_ids = insert_authors(cursor, authors_parsed)

    # Extract category data and insert, returning IDs
    categories = record.get('categories', '').split()
    category_ids = insert_categories(cursor, categories)

    # Extract paper details
    paper_id = record['id']
    title = record.get('title')
    submitter = record.get('submitter')
    comments = record.get('comments')
    journal_ref = record.get('journal-ref')
    doi = record.get('doi')
    report_no = record.get('report-no')
    abstract = record.get('abstract')
    update_date = record.get('update_date')

    # Insert paper details into papers table
    cursor.execute(
        """
        INSERT INTO papers (paper_id, title, submitter, comments, journal_ref, doi, report_no, abstract, update_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (paper_id) DO NOTHING;
        """,
        (paper_id, title, submitter, comments, journal_ref, doi, report_no, abstract, update_date)
    )

    # Insert relationships into paper_authors
    for author_id in author_ids:
        cursor.execute(
            "INSERT INTO paper_authors (paper_id, author_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (paper_id, author_id)
        )

    # Insert relationships into paper_categories
    for category_id in category_ids:
        cursor.execute(
            "INSERT INTO paper_categories (paper_id, category_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (paper_id, category_id)
        )


# Function to load data from JSON record
import json
import psycopg2

# Main function to be called by Airflow
def load_data():
    # Establish database connection
    try:
        with psycopg2.connect(
            dbname='arxiv',
            user='admin',
            password='admin',
            host='host.docker.internal'
        ) as conn:
            with conn.cursor() as cursor:
                with open('archive/data.json', 'r', encoding='utf-8') as file:
                    for line in file:
                        try:
                            record = json.loads(line)
                            process_record(cursor, record)
                        except json.JSONDecodeError as e:
                            print(f"Failed to parse JSON line: {e}")
                        except Exception as e:
                            print(f"An error occurred: {e}")
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        raise

# Airflow DAG and task definition
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'load_data_dag',
    default_args=default_args,
    description='A simple DAG to load data',
    schedule_interval=timedelta(days=1),
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

load_data_task

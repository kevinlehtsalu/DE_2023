from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import os
import psycopg2

def get_db_connection():
    return psycopg2.connect(
        dbname='arxiv',
        user='admin',
        password='admin',
        host='arxiv_postgres'
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'import_data',
    default_args=default_args,
    description='Import data to postgres database',
    schedule_interval=timedelta(days=1),
)

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

def read_and_process_json_file():
    file_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'full_data.json')
    conn = get_db_connection()
    try:
        with open(file_path, 'r') as file:
            record_count = 0
            for line in file:
                if record_count < 10:
                    data = json.loads(line)
                    # Insert into papers table
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO papers (paper_id, title, submitter, comments, journal_ref, doi, report_no, abstract, update_date)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (paper_id) DO NOTHING;
                        """, (
                            data['id'],
                            data['title'],
                            data['submitter'],
                            data['comments'],
                            data['journal-ref'],
                            data['doi'],
                            data['report-no'],
                            data['abstract'],
                            datetime.strptime(data['update_date'], "%Y-%m-%d").date() if data['update_date'] else None
                        ))

                        # Insert authors and get their IDs
                        author_ids = insert_authors(cursor, data.get('authors_parsed', []))

                        # Insert categories and get their IDs
                        categories = data.get('categories', '').split()
                        category_ids = insert_categories(cursor, categories)

                        # Insert paper-authors relationships
                        for author_id in author_ids:
                            cursor.execute(
                                """
                                INSERT INTO paper_authors (paper_id, author_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                                """,
                                (data['id'], author_id)
                            )

                        # Insert paper-categories relationships
                        for category_id in category_ids:
                            cursor.execute(
                                """
                                INSERT INTO paper_categories (paper_id, category_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                                """,
                                (data['id'], category_id)
                            )

                    conn.commit()
                    record_count += 1
                else:
                    break
    except FileNotFoundError:
        print(f"The file {file_path} was not found.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {file_path}.")
    except Exception as e:
        print(f"Error reading JSON file: {e}")
    finally:
        conn.close()



t1 = PythonOperator(
    task_id='read_and_process_json_file',
    python_callable=read_and_process_json_file,  # Corrected to the proper function name
    dag=dag,
)

# Set the order of execution
t1

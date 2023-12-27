import psycopg2
import requests

# Database connection parameters
db_params = {
    'dbname': 'arxiv',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost'
}

# Function to fetch citation data from Crossref
def fetch_citation_data(doi):
    try:
        url = f"https://api.crossref.org/works/{doi}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        citation_count = data['message'].get('is-referenced-by-count', 0)
        return citation_count
    except requests.exceptions.RequestException as e:
        print(f"Request to Crossref API failed: {e}")
        return 0

# Main script to update database with citation counts
def update_citation_counts():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Fetch a limited number of papers along with their DOIs
        cur.execute("SELECT paper_id, doi FROM papers LIMIT 30")
        papers = cur.fetchall()

        # Update each paper with citation count
        for paper_id, doi in papers:
            if doi:
                citation_count = fetch_citation_data(doi)
                update_query = "UPDATE papers SET citation_count = %s WHERE paper_id = %s"
                cur.execute(update_query, (citation_count, paper_id))

        # Commit changes and close connection
        conn.commit()
        print("Database update complete")

    except psycopg2.DatabaseError as e:
        print(f"Database error: {e}")

    finally:
        if conn:
            cur.close()
            conn.close()

# Run the script
update_citation_counts()

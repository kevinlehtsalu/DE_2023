from neo4j import GraphDatabase

uri = "neo4j://localhost:7687"
username = "neo4j"
password = "123456789"

driver = GraphDatabase.driver(uri, auth=(username, password))

def import_data(session):
    # Import Authors
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///authors.csv' AS row
        MERGE (a:Author {author_id: toInteger(row.author_id)})
        ON CREATE SET a.last_name = row.last_name, 
                       a.first_name = row.first_name, 
                       a.middle_name = row.middle_name
        """)
    except Exception as e:
        print(f"Error importing authors: {e}")

    # Import Categories
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///categories.csv' AS row
        MERGE (c:Category {category_id: toInteger(row.category_id)})
        ON CREATE SET c.category_name = row.category_name
        """)
    except Exception as e:
        print(f"Error importing categories: {e}")

    # Import Papers
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///papers.csv' AS row
        MERGE (p:Paper {paper_id: row.paper_id})
        ON CREATE SET p.title = row.title,
                       p.submitter = row.submitter,
                       p.comments = row.comments,
                       p.journal_ref = row.journal_ref,
                       p.doi = row.doi,
                       p.report_no = row.report_no,
                       p.abstract = row.abstract,
                       p.update_date = row.update_date,
                       p.citation_count = toInteger(row.citation_count)
        """)
    except Exception as e:
        print(f"Error importing papers: {e}")

    # Import Versions
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///versions.csv' AS row
        MERGE (v:Version {version_id: toInteger(row.version_id)})
        ON CREATE SET v.version = row.version,
                       v.created_date = row.created_date
        """)
    except Exception as e:
        print(f"Error importing versions: {e}")

    # Create Paper-Author Relationships
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_authors.csv' AS row
        MATCH (p:Paper {paper_id: row.paper_id})
        MATCH (a:Author {author_id: toInteger(row.author_id)})
        MERGE (p)-[:WRITTEN_BY]->(a)
        """)
    except Exception as e:
        print(f"Error creating paper-author relationships: {e}")

    # Create Paper-Category Relationships
    try:
        session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_categories.csv' AS row
        MATCH (p:Paper {paper_id: row.paper_id})
        MATCH (c:Category {category_id: toInteger(row.category_id)})
        MERGE (p)-[:BELONGS_TO]->(c)
        """)
    except Exception as e:
        print(f"Error creating paper-category relationships: {e}")

# Running the import
with driver.session() as session:
    print("Starting data import...")
    import_data(session)
    print("Data import completed.")

driver.close()

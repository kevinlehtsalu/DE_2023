FROM apache/airflow:latest

USER root

# Install system dependencies (if any)
RUN apt-get update -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch to the airflow user
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary neo4j

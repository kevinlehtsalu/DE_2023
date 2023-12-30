FROM apache/airflow:latest

# Switch to root user to perform privileged operations
USER root

# Install system dependencies (if any)
RUN apt-get update -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create the shared directory and set correct permissions
# The 'airflow' user and group already exist in the base image
RUN mkdir -p /opt/airflow/shared && \
    chown -R airflow /opt/airflow/shared

# Switch back to the airflow user for installing Python dependencies
USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary neo4j

# The container already runs as the airflow user by default

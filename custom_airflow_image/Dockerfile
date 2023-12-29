FROM apache/airflow:latest
USER root
RUN apt-get update -y \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && pip3 install psycopg2-binary \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

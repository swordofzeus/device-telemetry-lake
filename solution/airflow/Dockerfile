# Supplied starter image for the base Airflow control plane. Add pinned task
# dependencies here when they are required by the candidate-owned DAGs.
FROM apache/airflow:3.3.0

RUN pip install --no-cache-dir \
    apache-airflow-providers-trino==6.6.1

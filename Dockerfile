FROM apache/airflow:3.3.0

# Dépendances nécessaires pour exécuter l'ingestion dlt et dbt depuis les
# tasks Airflow. Dupliqué depuis pyproject.toml (pas de dépendance dev/pytest
# ici, inutile dans le runtime Airflow).
RUN pip install --no-cache-dir \
    "dbt-bigquery>=1.12.0" \
    "dbt-core>=1.12.0" \
    "dbt-duckdb>=1.10.1" \
    "dlt[bigquery,duckdb]>=1.29.1" \
    "duckdb>=1.5.5" \
    "requests>=2.34.2"

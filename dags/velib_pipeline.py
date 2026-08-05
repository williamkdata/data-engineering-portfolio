"""Pipeline Velib' : ingestion (Velib' + meteo) -> dbt run -> dbt test.

Cadence horaire : alignee sur le grain des marts (snapshot_hour, tronque a
l'heure cote SQL) et sur la resolution horaire des donnees Open-Meteo.
Cible BigQuery en continu (APP_ENV=bigquery / dbt --target prod, qui bascule
automatiquement sur le compte de service via DBT_GCP_KEYFILE dans ce
container) : le compte de service dedie (Partie 2) est le seul mecanisme
d'authentification utilisable sans supervision humaine.

max_active_runs=1 : deux executions concurrentes de ce DAG (ex. un run manuel
declenche pendant qu'un run planifie tourne encore) peuvent toutes les deux
faire un APPEND sur les memes tables brutes en meme temps -- constate en
pratique pendant le developpement (1518 lignes dupliquees, un test dbt unique
a echoue). Ce parametre empeche Airflow de demarrer un nouveau run tant que
le precedent n'est pas termine.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="velib_pipeline",
    description="Ingestion Velib' + meteo -> dbt snapshot -> dbt run -> dbt test",
    schedule="0 * * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["velib"],
) as dag:

    ingest_velib = BashOperator(
        task_id="ingest_velib",
        bash_command="python ingestion/velib_gbfs.py",
        cwd="/opt/airflow",
        env={"APP_ENV": "bigquery"},
        append_env=True,
    )

    ingest_meteo = BashOperator(
        task_id="ingest_meteo",
        bash_command="python ingestion/meteo.py",
        cwd="/opt/airflow",
        env={"APP_ENV": "bigquery"},
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="dbt snapshot --project-dir dbt --target prod",
        cwd="/opt/airflow",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir dbt --target prod",
        cwd="/opt/airflow",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir dbt --target prod",
        cwd="/opt/airflow",
    )

    [ingest_velib, ingest_meteo] >> dbt_snapshot >> dbt_run >> dbt_test

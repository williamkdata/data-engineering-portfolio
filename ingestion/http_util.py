import logging
import os

import dlt
import requests

logger = logging.getLogger(__name__)


def fetch_json(url: str) -> dict:
    logger.info("GET %s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def get_destination(duckdb_path: str):
    """Retourne la destination dlt selon APP_ENV (duckdb par défaut, ou bigquery)."""
    if os.environ.get("APP_ENV", "duckdb") == "bigquery":
        return dlt.destinations.bigquery(location="europe-west9")
    return dlt.destinations.duckdb(credentials=duckdb_path)
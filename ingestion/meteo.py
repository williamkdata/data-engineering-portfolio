import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import dlt
import duckdb
from http_util import fetch_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OPENMETEO_DISCOVERY_URL = "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&hourly=temperature_2m,windspeed_10m,precipitation"
DB_PATH = "data/velib.duckdb"
DATASET_NAME = "meteo_raw"

@dlt.resource(name="weather", write_disposition="append")
def get_weather():
    """Récupère la météo actuelle à Paris (pour enrichir le statut des stations)."""
    url = OPENMETEO_DISCOVERY_URL
    payload = fetch_json(url)
    data = payload["hourly"]
    ingested_at = datetime.now(timezone.utc).isoformat()
    resultat = [{"time" : time_weather, "temperature_2m": temperature, "windspeed_10m": windspeed, "precipitation": precipitation, "ingested_at": ingested_at} for time_weather, temperature, windspeed, precipitation in zip(data["time"], data["temperature_2m"], data["windspeed_10m"], data["precipitation"])]
    yield resultat

def print_control_query() -> None:
    with duckdb.connect(DB_PATH) as conn:
        nb_rows = conn.execute(
            f"SELECT COUNT(*) FROM {DATASET_NAME}.weather"
        ).fetchone()[0]
        min_time = conn.execute(
            f"SELECT MIN(time) FROM {DATASET_NAME}.weather"
        ).fetchone()[0]
        max_time = conn.execute(
            f"SELECT MAX(time) FROM {DATASET_NAME}.weather"
        ).fetchone()[0]

    print(f"Nombre de lignes dans la table weather : {nb_rows}")
    print(f"Date minimale : {min_time}")
    print(f"Date maximale : {max_time}")

def main():
    start = time.perf_counter()
    Path("data").mkdir(exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="meteo_pipeline",
        destination=dlt.destinations.duckdb(credentials=DB_PATH),
        dataset_name=DATASET_NAME,
    )
    load_info = pipeline.run(
        [
            get_weather(),
        ]
    )
    logger.info(load_info)
    print_control_query()
    logger.info("Run terminé en %.1fs", time.perf_counter() - start)

if __name__ == "__main__":
    main()
"""Ingestion GBFS Vélib' Métropole -> DuckDB (Milestone M1).

Deux flux :
- station_information : photo courante des stations (replace à chaque run).
- station_status : disponibilité en temps réel (append à chaque run, avec un
  timestamp d'ingestion -> on construit un historique en relançant le script).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import dlt
import duckdb
from http_util import fetch_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GBFS_DISCOVERY_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json"
DB_PATH = "data/velib.duckdb"
DATASET_NAME = "velib_raw"


def get_feed_urls(discovery_url: str = GBFS_DISCOVERY_URL) -> dict[str, str]:
    """Lit le document de découverte GBFS et retourne {nom_du_flux: url}."""
    discovery = fetch_json(discovery_url)
    languages = discovery["data"]
    # certains flux GBFS mettent "feeds" directement sous "data", d'autres le
    # nichent sous une langue ("en", "fr", ...) : on gère les deux cas.
    feeds = languages.get("feeds") or next(iter(languages.values()))["feeds"]
    return {feed["name"]: feed["url"] for feed in feeds}


# @dlt.resource transforme ce générateur en "producteur de table" dlt : chaque
# item yield devient une ligne, dlt déduit le schéma tout seul et écrit dans la
# destination configurée sur le pipeline (ici DuckDB). write_disposition
# "replace" = la table est vidée puis rechargée à chaque run : logique pour
# station_information, qui ne change quasiment jamais.
@dlt.resource(name="station_information", write_disposition="replace")
def station_information(feed_url: str):
    payload = fetch_json(feed_url)
    stations = payload["data"]["stations"]
    logger.info("station_information : %d stations", len(stations))
    yield stations


# write_disposition "append" conserve tout ce qui a été chargé précédemment et
# ajoute les nouvelles lignes par-dessus : c'est ce qu'on veut pour le statut,
# où chaque run est une photo à un instant T qu'on veut accumuler, pas écraser.
@dlt.resource(name="station_status", write_disposition="append")
def station_status(feed_url: str):
    payload = fetch_json(feed_url)
    stations = payload["data"]["stations"]
    ingested_at = datetime.now(timezone.utc).isoformat()
    for station in stations:
        station["ingested_at"] = ingested_at
    logger.info("station_status : %d stations (ingested_at=%s)", len(stations), ingested_at)
    yield stations


def print_control_query() -> None:
    with duckdb.connect(DB_PATH) as conn:
        nb_stations = conn.execute(
            f"select count(*) from {DATASET_NAME}.station_information"
        ).fetchone()[0]
        last_run = conn.execute(
            f"select max(ingested_at) from {DATASET_NAME}.station_status"
        ).fetchone()[0]
        bikes_last_run = conn.execute(
            f"select sum(num_bikes_available) from {DATASET_NAME}.station_status "
            "where ingested_at = ?",
            [last_run],
        ).fetchone()[0]
        total_status_rows = conn.execute(
            f"select count(*) from {DATASET_NAME}.station_status"
        ).fetchone()[0]

    print(f"Stations connues        : {nb_stations}")
    print(f"Vélos dispos (ce run)   : {bikes_last_run} (à {last_run})")
    print(f"Historique station_status : {total_status_rows} lignes cumulées")


def main() -> None:
    start = time.perf_counter()
    Path("data").mkdir(exist_ok=True)

    feed_urls = get_feed_urls()

    pipeline = dlt.pipeline(
        pipeline_name="velib_gbfs",
        destination=dlt.destinations.duckdb(credentials=DB_PATH),
        dataset_name=DATASET_NAME,
    )

    load_info = pipeline.run(
        [
            station_information(feed_urls["station_information"]),
            station_status(feed_urls["station_status"]),
        ]
    )
    logger.info(load_info)

    print_control_query()

    logger.info("Run terminé en %.1fs", time.perf_counter() - start)


if __name__ == "__main__":
    main()

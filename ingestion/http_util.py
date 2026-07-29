import logging

import requests

logger = logging.getLogger(__name__)


def fetch_json(url: str) -> dict:
    logger.info("GET %s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()
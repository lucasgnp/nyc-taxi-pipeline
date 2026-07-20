"""Download da tabela de referencia de zonas de taxi da NYC TLC.

Diferente dos parquets de corridas, o lookup de zonas é um CSV único e estável,
então não precisa de particionamento por competência. Baixa uma vez para a
camada bronze do lake.
"""

from __future__ import annotations
import logging
import requests
from src.common import config

logger = logging.getLogger(__name__)

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
TIMEOUT = (10, 60)


def download_zone_lookup() -> str:
    """Baixa o CSV de zonas para a bronze do lake. Retorna o caminho."""
    destination = config.LAKE_ROOT / "bronze" / "taxi_zone_lookup.csv"
    destination.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Baixando lookup de zonas de %s", ZONE_LOOKUP_URL)
    response = requests.get(ZONE_LOOKUP_URL, timeout=TIMEOUT)
    response.raise_for_status()

    destination.write_bytes(response.content)
    logger.info("Lookup de zonas gravado em %s (%d bytes)",
                destination, len(response.content))
    return str(destination)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    download_zone_lookup()


if __name__ == "__main__":
    main()
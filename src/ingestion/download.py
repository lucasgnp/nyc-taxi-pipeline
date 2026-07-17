"""Download incremental dos parquets de Yellow Taxi da NYC TLC.

Cada execucao trata UMA competencia (ano/mes). A DAG do Airflow chama este
modulo passando a competencia derivada do proprio intervalo de execucao, o que
torna o backfill possivel. Rodar de novo a mesma competencia nao rebaixa o
arquivo se ele ja estiver integro.
"""

from __future__ import annotations
import argparse
import logging
import time
from pathlib import Path
import requests
from src.common import config

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8 * 1024 * 1024   # 8 MB por bloco
MAX_RETRIES = 3
BACKOFF_BASE = 2               # segundos: 2, 4, 8...
TIMEOUT = (10, 300)            # (conexao, leitura)


def _remote_size(url: str) -> int | None:
    """Tamanho do arquivo remoto em bytes, ou None se o servidor nao informar."""
    response = requests.head(url, timeout=TIMEOUT, allow_redirects=True)
    response.raise_for_status()
    content_length = response.headers.get("Content-Length")
    return int(content_length) if content_length else None


def _is_up_to_date(destination: Path, expected_size: int | None) -> bool:
    """Decide se o arquivo local ja esta integro e dispensa novo download."""
    if not destination.exists():
        return False

    if expected_size is None:
        logger.warning(
            "Origem nao informou Content-Length. Assumindo o arquivo local "
            "como valido: %s", destination
        )
        return True

    actual_size = destination.stat().st_size
    if actual_size == expected_size:
        return True

    logger.warning(
        "Arquivo local com tamanho divergente (local=%s, origem=%s). "
        "Vou baixar de novo: %s", actual_size, expected_size, destination
    )
    return False


def _stream_to_file(url: str, destination: Path) -> None:
    """Baixa em blocos para um arquivo temporario e so entao renomeia."""
    tmp_path = destination.with_name(destination.name + ".tmp")

    with requests.get(url, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        with tmp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                handle.write(chunk)

    # rename e atomico no mesmo sistema de arquivos: ou o parquet final existe
    # inteiro, ou nao existe. Nunca pela metade.
    tmp_path.replace(destination)


def download_month(year: int, month: int) -> Path:
    """Garante o parquet de uma competencia na camada bronze do lake.

    Retorna o caminho do arquivo, tenha ele sido baixado agora ou nao.
    """
    url = config.parquet_url(year, month)
    destination = config.bronze_file_path(year, month)
    destination.parent.mkdir(parents=True, exist_ok=True)

    expected_size = _remote_size(url)

    if _is_up_to_date(destination, expected_size):
        logger.info("Competencia %s-%02d ja presente. Nada a fazer.", year, month)
        return destination

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(
                "Baixando %s (tentativa %s de %s)", url, attempt, MAX_RETRIES
            )
            _stream_to_file(url, destination)
            break
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                logger.error("Falha definitiva ao baixar %s: %s", url, exc)
                raise
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Falha ao baixar %s: %s. Nova tentativa em %ss.", url, exc, wait
            )
            time.sleep(wait)

    final_size = destination.stat().st_size
    if expected_size is not None and final_size != expected_size:
        destination.unlink()
        raise IOError(
            f"Download incompleto de {url}: "
            f"esperado {expected_size} bytes, recebido {final_size}."
        )

    logger.info(
        "Competencia %s-%02d gravada em %s (%.1f MB)",
        year, month, destination, final_size / 1024 / 1024,
    )
    return destination


def main() -> None:
    """Permite rodar o modulo direto pelo terminal, sem Airflow."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13))
    args = parser.parse_args()
    download_month(args.year, args.month)


if __name__ == "__main__":
    main()
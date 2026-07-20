"""Carga da tabela de referência de zonas para a bronze do warehouse.

Idempotente: trunca e recarrega. Como é uma tabela de referência pequena e
estável, o custo do reload completo é irrelevante.
"""

from __future__ import annotations
import logging
from sqlalchemy import create_engine, text # type: ignore
from src.common import config

logger = logging.getLogger(__name__)

TARGET_TABLE = "bronze.brz_taxi_zone_lookup"


def load_zone_lookup() -> int:
    """Carrega o CSV de zonas na bronze. Retorna o numero de linhas."""
    source = config.LAKE_ROOT / "bronze" / "taxi_zone_lookup.csv"
    if not source.exists():
        raise FileNotFoundError(
            f"CSV de zonas nao encontrado: {source}. Rode o download antes."
        )

    engine = create_engine(config.sqlalchemy_url())
    try:
        with engine.begin() as connection:
            # TRUNCATE + COPY na mesma transacao: idempotente e atomico.
            connection.execute(text(f"TRUNCATE {TARGET_TABLE}"))

            raw = connection.connection
            cursor = raw.cursor()
            with source.open("r") as handle:
                cursor.copy_expert(
                    f"COPY {TARGET_TABLE} (location_id, borough, zone, service_zone) "
                    f"FROM STDIN WITH (FORMAT csv, HEADER true)",
                    handle,
                )
            cursor.close()

            count = connection.execute(
                text(f"SELECT count(*) FROM {TARGET_TABLE}")
            ).scalar()
    finally:
        engine.dispose()

    logger.info("Lookup de zonas carregado em %s: %d linhas.", TARGET_TABLE, count)
    return count


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    load_zone_lookup()


if __name__ == "__main__":
    main()
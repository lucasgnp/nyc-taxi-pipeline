"""Carga da camada bronze: parquet do lake para bronze.yellow_tripdata.

Trata UMA competencia por execucao. A carga e idempotente: apaga a competencia
inteira e recarrega, tudo dentro da mesma transacao. Nenhum valor da origem e
transformado aqui; as unicas colunas acrescentadas sao de controle.
"""

from __future__ import annotations
import argparse
import csv
import io
import logging
from pathlib import Path
import pyarrow.parquet as pq # type: ignore
from sqlalchemy import create_engine, text # type: ignore
from src.common import config

logger = logging.getLogger(__name__)

TARGET_TABLE = "bronze.brz_yellow_tripdata"
BATCH_SIZE = 200_000

# Colunas da origem, na ordem e com o nome exatos do parquet da TLC.
SOURCE_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    "cbd_congestion_fee",
]

CONTROL_COLUMNS = ["year", "month", "source_file"]

# No destino tudo e minusculo. A ordem tem que casar com a do COPY.
TARGET_COLUMNS = [c.lower() for c in SOURCE_COLUMNS] + CONTROL_COLUMNS


def _validate_schema(parquet_file: pq.ParquetFile, path: Path) -> None:
    """Falha alto se o parquet nao tiver exatamente as colunas esperadas.

    Os arquivos da TLC ja mudaram de schema entre competencias em outros anos.
    Prefiro quebrar a carga a gravar dado silenciosamente errado.
    """
    found = list(parquet_file.schema_arrow.names)
    if found == SOURCE_COLUMNS:
        return

    missing = sorted(set(SOURCE_COLUMNS) - set(found))
    extra = sorted(set(found) - set(SOURCE_COLUMNS))
    raise ValueError(
        f"Schema inesperado em {path.name}.\n"
        f"  colunas faltando: {missing or 'nenhuma'}\n"
        f"  colunas a mais:   {extra or 'nenhuma'}\n"
        f"  esperado: {SOURCE_COLUMNS}\n"
        f"  encontrado: {found}"
    )


def _copy_batches(raw_connection, parquet_file: pq.ParquetFile,
                  year: int, month: int, source_file: str) -> int:
    """Copia o parquet em lotes para o Postgres via COPY."""
    cursor = raw_connection.cursor()
    columns = ", ".join(TARGET_COLUMNS)
    copy_sql = f"COPY {TARGET_TABLE} ({columns}) FROM STDIN WITH (FORMAT csv, NULL '')"

    total = 0
    for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
        # integer_object_nulls preserva inteiros com nulo como inteiro.
        # Sem isso o pandas promove a coluna para float e o COPY receberia
        # "1.0" onde a tabela espera BIGINT.
        frame = batch.to_pandas(integer_object_nulls=True)
        frame["year"] = year
        frame["month"] = month
        frame["source_file"] = source_file

        buffer = io.StringIO()
        frame.to_csv(
            buffer,
            index=False,
            header=False,
            na_rep="",
            quoting=csv.QUOTE_MINIMAL,
        )
        buffer.seek(0)
        cursor.copy_expert(copy_sql, buffer)

        total += len(frame)
        logger.info("   ... %s linhas copiadas", f"{total:,}")

    cursor.close()
    return total


def load_month(year: int, month: int) -> int:
    """Carrega uma competencia do lake para a bronze do warehouse."""
    path = config.bronze_file_path(year, month)
    if not path.exists():
        raise FileNotFoundError(
            f"Parquet nao encontrado: {path}. Rode o download desta competencia antes."
        )

    parquet_file = pq.ParquetFile(path)
    _validate_schema(parquet_file, path)

    engine = create_engine(config.sqlalchemy_url())
    try:
        # engine.begin() abre uma transacao e faz commit no fim do bloco,
        # ou rollback se qualquer coisa levantar excecao. E isso que garante
        # que nunca existe um estado com a competencia apagada e nao recarregada.
        with engine.begin() as connection:
            deleted = connection.execute(
                text(f"DELETE FROM {TARGET_TABLE} WHERE year = :year AND month = :month"),
                {"year": year, "month": month},
            ).rowcount

            if deleted:
                logger.info(
                    "Competencia %s-%02d ja existia: %s linhas removidas para recarga.",
                    year, month, f"{deleted:,}",
                )

            inserted = _copy_batches(
                connection.connection, parquet_file, year, month, path.name
            )
    finally:
        engine.dispose()

    logger.info(
        "Competencia %s-%02d carregada em %s: %s linhas.",
        year, month, TARGET_TABLE, f"{inserted:,}",
    )
    return inserted


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13))
    args = parser.parse_args()
    load_month(args.year, args.month)


if __name__ == "__main__":
    main()
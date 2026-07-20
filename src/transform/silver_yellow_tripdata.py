"""Job PySpark: camada bronze do lake para silver.yellow_tripdata no warehouse.

Lê os parquets crus de UMA competência, calcula as colunas derivadas, avalia as
regras de qualidade e grava o resultado no Postgres.

Nenhuma linha é descartada. Anomalia é sinalizada, não removida: decidir o que
fazer com ela é responsabilidade de quem consome a camada.
"""

from __future__ import annotations
import argparse
import calendar
import logging
from datetime import date, timedelta
from pyspark.sql import DataFrame, SparkSession # type: ignore
from pyspark.sql import functions as F # type: ignore
from pyspark.sql import types as T # type: ignore
from sqlalchemy import create_engine, text # type: ignore
from src.common import config

logger = logging.getLogger(__name__)

TARGET_TABLE = "silver.slv_yellow_tripdata"
SOURCE_PAYMENT_TABLE = "bronze.brz_payment_type"

MAX_DURATION_MINUTES = 6 * 60
MAX_DISTANCE_MILES = 100
COMPETENCIA_TOLERANCE_DAYS = 1

MONEY_COLUMNS = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee",
]

# Schema explicito. Os arquivos da TLC ja mudaram de tipo entre competencias em
# outros anos; deixar o Spark inferir seria aceitar que a origem mude debaixo do
# pipeline sem ninguem perceber.
BRONZE_SCHEMA = T.StructType([
    T.StructField("VendorID", T.IntegerType()),
    T.StructField("tpep_pickup_datetime", T.TimestampNTZType()),
    T.StructField("tpep_dropoff_datetime", T.TimestampNTZType()),
    T.StructField("passenger_count", T.LongType()),
    T.StructField("trip_distance", T.DoubleType()),
    T.StructField("RatecodeID", T.LongType()),
    T.StructField("store_and_fwd_flag", T.StringType()),
    T.StructField("PULocationID", T.IntegerType()),
    T.StructField("DOLocationID", T.IntegerType()),
    T.StructField("payment_type", T.LongType()),
    T.StructField("fare_amount", T.DoubleType()),
    T.StructField("extra", T.DoubleType()),
    T.StructField("mta_tax", T.DoubleType()),
    T.StructField("tip_amount", T.DoubleType()),
    T.StructField("tolls_amount", T.DoubleType()),
    T.StructField("improvement_surcharge", T.DoubleType()),
    T.StructField("total_amount", T.DoubleType()),
    T.StructField("congestion_surcharge", T.DoubleType()),
    T.StructField("Airport_fee", T.DoubleType()),
    T.StructField("cbd_congestion_fee", T.DoubleType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("silver_yellow_tripdata")
        .config("spark.jars", config.JDBC_JAR_PATH)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, year: int, month: int) -> DataFrame:
    """Le os parquets de uma competencia direto da particao dela."""
    path = str(config.bronze_partition_path(year, month))
    logger.info("Lendo parquet de %s", path)
    frame = spark.read.schema(BRONZE_SCHEMA).parquet(path)
    # Bronze é cru e tem casing inconsistente na origem. Padronizo aqui.
    return frame.toDF(*[c.lower() for c in frame.columns])


def read_payment_type(spark: SparkSession) -> DataFrame:
    """Le a tabela de referencia de pagamentos do warehouse."""
    return (
        spark.read.format("jdbc")
        .option("url", config.jdbc_url())
        .option("dbtable", SOURCE_PAYMENT_TABLE)
        .option("user", config.DW_USER)
        .option("password", config.DW_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def quality_rules(year: int, month: int) -> list[tuple[str, "F.Column"]]:
    """Regras de qualidade. Cada tupla e (nome do motivo, condicao de violacao)."""
    last_day = calendar.monthrange(year, month)[1]
    floor_date = date(year, month, 1) - timedelta(days=COMPETENCIA_TOLERANCE_DAYS)
    ceiling_date = date(year, month, last_day) + timedelta(days=COMPETENCIA_TOLERANCE_DAYS)

    return [
        ("duracao_acima_6h",
         F.col("trip_duration_minutes") > MAX_DURATION_MINUTES),

        ("distancia_acima_100_milhas",
         F.col("trip_distance") > MAX_DISTANCE_MILES),

        ("duracao_nao_positiva",
         F.col("trip_duration_minutes") <= 0),

        ("distancia_nao_positiva",
         F.col("trip_distance") <= 0),

        ("valor_total_negativo",
         F.col("total_amount") < 0),

        # Tolerancia de 1 dia: corrida que comeca 23h59 do dia 31 e aparece no
        # arquivo do mes seguinte é normal. Corrida de 2007 num arquivo de 2025
        # é lixo. A margem separa as duas.
        ("embarque_fora_da_competencia",
         (F.col("pickup_date") < F.lit(floor_date))
         | (F.col("pickup_date") > F.lit(ceiling_date))),
    ]


def transform(spark: SparkSession, year: int, month: int) -> DataFrame:
    trips = read_bronze(spark, year, month)
    payments = read_payment_type(spark)

    trips = (
        trips
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year_month", F.date_format("tpep_pickup_datetime", "yyyyMM"))
        .withColumn("store_and_fwd_flag", F.upper(F.trim(F.col("store_and_fwd_flag"))))
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
                 - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0,
                2,
            ),
        )
    )

    # broadcast: a tabela de pagamentos tem 6 linhas. Sem isso o Spark faria um
    # shuffle de 3,5 milhões de linhas para juntar com uma tabela insignificante.
    trips = trips.join(
        F.broadcast(payments),
        on=trips["payment_type"] == payments["payment_type"],
        how="left",
    ).drop(payments["payment_type"])

    rules = quality_rules(year, month)
    reasons = [F.when(condition, F.lit(name)) for name, condition in rules]
    # concat_ws ignora nulos, entao so os motivos violados entram na string.
    joined_reasons = F.concat_ws(",", *reasons)

    trips = (
        trips
        .withColumn(
            "invalid_reason",
            F.when(joined_reasons == "", None).otherwise(joined_reasons),
        )
        .withColumn("is_valid_trip", F.col("invalid_reason").isNull())
        .withColumn(
            "is_incomplete_record",
            F.coalesce(F.col("passenger_count") == 0, F.lit(False)),
        )
        .withColumn("year", F.lit(year).cast(T.ShortType()))
        .withColumn("month", F.lit(month).cast(T.ShortType()))
    )

    # A bronze guarda dinheiro em double para espelhar a origem. Aqui vira
    # decimal exato: ticket medio calculado em float acumula erro.
    for column in MONEY_COLUMNS:
        trips = trips.withColumn(column, F.col(column).cast(T.DecimalType(12, 2)))

    trips = (
        trips
        .withColumn("trip_distance", F.col("trip_distance").cast(T.DecimalType(10, 2)))
        .withColumn("trip_duration_minutes", F.col("trip_duration_minutes").cast(T.DecimalType(10, 2)))
        .withColumn("passenger_count", F.col("passenger_count").cast(T.IntegerType()))
        .withColumn("ratecodeid", F.col("ratecodeid").cast(T.IntegerType()))
        .withColumn("payment_type", F.col("payment_type").cast(T.IntegerType()))
    )

    return trips.select(
        "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag",
        "pulocationid", "dolocationid",
        "payment_type", "payment_description", "is_valid_payment",
        *MONEY_COLUMNS,
        "pickup_date", "pickup_year_month", "trip_duration_minutes",
        "is_valid_trip", "is_incomplete_record", "invalid_reason",
        "year", "month",
    )


def delete_competencia(year: int, month: int) -> int:
    """Apaga a competencia antes da regravação. Chave é o arquivo, não o embarque."""
    engine = create_engine(config.sqlalchemy_url())
    try:
        with engine.begin() as connection:
            deleted = connection.execute(
                text(f"DELETE FROM {TARGET_TABLE} WHERE year = :year AND month = :month"),
                {"year": year, "month": month},
            ).rowcount
    finally:
        engine.dispose()
    return deleted


def write_silver(frame: DataFrame) -> None:
    (
        frame.write.format("jdbc")
        .option("url", config.jdbc_url())
        .option("dbtable", TARGET_TABLE)
        .option("user", config.DW_USER)
        .option("password", config.DW_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 50_000)
        .mode("append")
        .save()
    )


def run(year: int, month: int) -> None:
    spark = build_spark()
    try:
        frame = transform(spark, year, month)
        frame.cache()
        total = frame.count()
        invalid = frame.filter(~F.col("is_valid_trip")).count()

        deleted = delete_competencia(year, month)
        if deleted:
            logger.info(
                "Competencia %s-%02d ja existia na silver: %s linhas removidas.",
                year, month, f"{deleted:,}",
            )

        write_silver(frame)

        logger.info(
            "Competencia %s-%02d gravada em %s: %s linhas, %s sinalizadas (%.2f%%).",
            year, month, TARGET_TABLE, f"{total:,}", f"{invalid:,}",
            100 * invalid / total if total else 0,
        )
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True, choices=range(1, 13))
    args = parser.parse_args()
    run(args.year, args.month)


if __name__ == "__main__":
    main()
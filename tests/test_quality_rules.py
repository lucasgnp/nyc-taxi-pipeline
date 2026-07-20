"""Testes das regras de qualidade da silver e da unicidade da chave de negócio.

Usa uma SparkSession local para exercitar a lógica exatamente como ela roda no
job, em vez de reimplementar as regras em Python puro (o que poderia divergir da
implementação real).
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.transform.silver_yellow_tripdata import quality_rules


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _apply_rules(df, year, month):
    """Aplica as regras e devolve o df com invalid_reason, como no job."""
    rules = quality_rules(year, month)
    reasons = [F.when(cond, F.lit(name)) for name, cond in rules]
    joined = F.concat_ws(",", *reasons)
    return df.withColumn(
        "invalid_reason",
        F.when(joined == "", None).otherwise(joined),
    )


def test_corrida_valida_nao_tem_motivo(spark):
    """Uma corrida normal nao deve ser sinalizada."""
    df = spark.createDataFrame(
        [(30.0, 5.0, 25.0, "2025-06-15")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    result = _apply_rules(df, 2025, 6).collect()[0]
    assert result["invalid_reason"] is None


def test_duracao_acima_6h_e_sinalizada(spark):
    """Duracao acima de 360 minutos deve disparar a regra."""
    df = spark.createDataFrame(
        [(400.0, 5.0, 25.0, "2025-06-15")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    result = _apply_rules(df, 2025, 6).collect()[0]
    assert "duracao_acima_6h" in result["invalid_reason"]


def test_distancia_acima_100_milhas_e_sinalizada(spark):
    df = spark.createDataFrame(
        [(30.0, 150.0, 25.0, "2025-06-15")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    result = _apply_rules(df, 2025, 6).collect()[0]
    assert "distancia_acima_100_milhas" in result["invalid_reason"]


def test_valor_negativo_e_sinalizado(spark):
    df = spark.createDataFrame(
        [(30.0, 5.0, -10.0, "2025-06-15")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    result = _apply_rules(df, 2025, 6).collect()[0]
    assert "valor_total_negativo" in result["invalid_reason"]


def test_multiplos_motivos_sao_concatenados(spark):
    """Uma corrida pode violar varias regras; todas devem aparecer."""
    df = spark.createDataFrame(
        [(-5.0, -2.0, -10.0, "2025-06-15")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    result = _apply_rules(df, 2025, 6).collect()[0]
    reason = result["invalid_reason"]
    assert "duracao_nao_positiva" in reason
    assert "distancia_nao_positiva" in reason
    assert "valor_total_negativo" in reason


def test_embarque_fora_da_competencia_com_tolerancia(spark):
    """Embarque muito antigo dispara; virada de mes (1 dia) nao dispara."""
    # Lixo temporal: embarque de 2007 num arquivo de 2025.
    df_lixo = spark.createDataFrame(
        [(30.0, 5.0, 25.0, "2007-12-05")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    r_lixo = _apply_rules(df_lixo, 2025, 6).collect()[0]
    assert "embarque_fora_da_competencia" in r_lixo["invalid_reason"]

    # Transbordo de virada: ultimo dia do mes anterior, dentro da tolerancia.
    df_virada = spark.createDataFrame(
        [(30.0, 5.0, 25.0, "2025-05-31")],
        ["trip_duration_minutes", "trip_distance", "total_amount", "pickup_date"],
    ).withColumn("pickup_date", F.to_date("pickup_date"))
    r_virada = _apply_rules(df_virada, 2025, 6).collect()[0]
    assert r_virada["invalid_reason"] is None
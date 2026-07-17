import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Origem dos dados
# ---------------------------------------------------------------------------

TLC_BASE_URL = os.getenv(
    "TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data",
)
TLC_DATASET = "yellow_tripdata"

# ---------------------------------------------------------------------------
# Data lake
# ---------------------------------------------------------------------------

LAKE_ROOT = Path(os.getenv("LAKE_ROOT", "/opt/airflow/data"))
BRONZE_DIR = LAKE_ROOT / "bronze" / TLC_DATASET

# ---------------------------------------------------------------------------
# Data warehouse
# ---------------------------------------------------------------------------

DW_HOST = os.getenv("DW_POSTGRES_HOST", "postgres-dw")
DW_PORT = int(os.getenv("DW_POSTGRES_PORT", "5432"))
DW_DB = os.getenv("DW_POSTGRES_DB", "nyc_taxi")
DW_USER = os.getenv("DW_POSTGRES_USER", "taxi")
DW_PASSWORD = os.getenv("DW_POSTGRES_PASSWORD", "taxi")

JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH", "/opt/spark/jars/postgresql-42.7.3.jar")


# ---------------------------------------------------------------------------
# Derivados
# ---------------------------------------------------------------------------

def parquet_filename(year: int, month: int) -> str:
    """Nome do arquivo da TLC para uma competencia."""
    return f"{TLC_DATASET}_{year}-{month:02d}.parquet"


def parquet_url(year: int, month: int) -> str:
    """URL publica do parquet de uma competencia."""
    return f"{TLC_BASE_URL}/{parquet_filename(year, month)}"


def bronze_partition_path(year: int, month: int) -> Path:
    """Pasta da particao Hive de uma competencia na bronze."""
    return BRONZE_DIR / f"year={year}" / f"month={month:02d}"


def bronze_file_path(year: int, month: int) -> Path:
    """Caminho completo do parquet de uma competencia na bronze."""
    return bronze_partition_path(year, month) / parquet_filename(year, month)


def jdbc_url() -> str:
    """URL JDBC do warehouse, usada pelo Spark."""
    return f"jdbc:postgresql://{DW_HOST}:{DW_PORT}/{DW_DB}"


def sqlalchemy_url() -> str:
    """URL SQLAlchemy do warehouse, usada pela carga da bronze."""
    return (
        f"postgresql+psycopg2://{DW_USER}:{DW_PASSWORD}"
        f"@{DW_HOST}:{DW_PORT}/{DW_DB}"
    )
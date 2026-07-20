"""DAG do pipeline NYC Yellow Taxi: bronze -> silver -> gold.

Cada execução processa UMA competência mensal, derivada da data lógica do
Airflow (nunca de datetime.now), o que torna o backfill possível e as tasks
reexecutaveis. O schedule é semanal conforme o enunciado; como o dado da TLC
é mensal, execuções da mesma competência são idempotentes e não corrompem
o resultado (cada camada faz delete+insert por competência).
"""

from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# A competência vem da data lógica do Airflow via template Jinja.
# {{ data_interval_start }} é o inicio do intervalo que a execução representa.
YEAR = "{{ data_interval_start.year }}"
MONTH = "{{ data_interval_start.month }}"

PROJECT_ROOT = "/opt/airflow"
DBT_DIR = f"{PROJECT_ROOT}/dbt"

default_args = {
    "owner": "lucas",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Pipeline medalhão de corridas de taxi amarelo da NYC TLC",
    schedule="@weekly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["nyc-taxi", "medallion"],
) as dag:

    # 1. Baixa o parquet da competência e grava na bronze do lake.
    download = BashOperator(
        task_id="download_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m src.ingestion.download --year {YEAR} --month {MONTH}"
        ),
    )

    # 2. Carrega o parquet na bronze do Postgres.
    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m src.ingestion.load_bronze --year {YEAR} --month {MONTH}"
        ),
    )

    # 3. Transforma bronze -> silver com as regras de qualidade (PySpark).
    silver = BashOperator(
        task_id="process_silver",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m src.transform.silver_yellow_tripdata --year {YEAR} --month {MONTH}"
        ),
    )

    # 4. Transforma silver -> gold (dbt): fato da competência e MV.
    gold = BashOperator(
        task_id="process_gold",
        bash_command=(
            f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} "
            f"--select fact_trips mv_trip_indicators "
            f"--vars '{{year: {YEAR}, month: {MONTH}}}'"
        ),
    )

    download >> load_bronze >> silver >> gold
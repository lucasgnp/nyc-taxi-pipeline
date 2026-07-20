-- Camada bronze: copia fiel dos parquets de Yellow Taxi da NYC TLC.
-- Tipos espelham o schema de origem, verificado nos arquivos de 2025-01 a 2025-06.
-- Nomes apenas em minusculas, para evitar identificadores com aspas no Postgres.
-- Nenhuma transformacao de valor acontece aqui.

CREATE TABLE IF NOT EXISTS bronze.brz_yellow_tripdata (
    -- Colunas da origem
    vendorid              INTEGER,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       BIGINT,
    trip_distance         DOUBLE PRECISION,
    ratecodeid            BIGINT,
    store_and_fwd_flag    TEXT,
    pulocationid          INTEGER,
    dolocationid          INTEGER,
    payment_type          BIGINT,
    fare_amount           DOUBLE PRECISION,
    extra                 DOUBLE PRECISION,
    mta_tax               DOUBLE PRECISION,
    tip_amount            DOUBLE PRECISION,
    tolls_amount          DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION,
    cbd_congestion_fee    DOUBLE PRECISION,

    -- Controle de ingestao
    year                  SMALLINT    NOT NULL, --índices em year e month serve para poder deletar mês e ano específico, caso precise
    month                 SMALLINT    NOT NULL,
    source_file           TEXT        NOT NULL,
    ingested_at           TIMESTAMP   NOT NULL DEFAULT now()
);

COMMENT ON TABLE bronze.brz_yellow_tripdata IS
    'Copia bruta das corridas de taxi amarelo da NYC TLC. Sem tratamento. '
    'Particionada logicamente por competencia (year, month).';

COMMENT ON COLUMN bronze.brz_yellow_tripdata.year IS 'Competencia derivada do arquivo de origem, nao do dado.';
COMMENT ON COLUMN bronze.brz_yellow_tripdata.month IS 'Competencia derivada do arquivo de origem, nao do dado.';
COMMENT ON COLUMN bronze.brz_yellow_tripdata.source_file IS 'Arquivo parquet que originou a linha. Rastreabilidade.';

CREATE INDEX IF NOT EXISTS idx_brz_yellow_tripdata_competencia
    ON bronze.brz_yellow_tripdata (year, month);
-- Camada silver: corridas tratadas, com regras de qualidade aplicadas.
-- Nenhuma linha e descartada. As anomalias sao sinalizadas, nao removidas:
-- decidir o que fazer com elas e responsabilidade de quem consome.

CREATE TABLE IF NOT EXISTS silver.slv_yellow_tripdata (
    -- Identificacao da corrida
    vendorid              INTEGER,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       INTEGER,
    trip_distance         NUMERIC(10, 2),
    ratecodeid            INTEGER,
    store_and_fwd_flag    TEXT,
    pulocationid          INTEGER,
    dolocationid          INTEGER,

    -- Pagamento
    payment_type          INTEGER,
    payment_description   TEXT,
    is_valid_payment      BOOLEAN,

    -- Valores, agora em NUMERIC
    fare_amount           NUMERIC(12, 2),
    extra                 NUMERIC(12, 2),
    mta_tax               NUMERIC(12, 2),
    tip_amount            NUMERIC(12, 2),
    tolls_amount          NUMERIC(12, 2),
    improvement_surcharge NUMERIC(12, 2),
    total_amount          NUMERIC(12, 2),
    congestion_surcharge  NUMERIC(12, 2),
    airport_fee           NUMERIC(12, 2),
    cbd_congestion_fee    NUMERIC(12, 2),

    -- Derivadas exigidas pelo desafio
    pickup_date           DATE,
    pickup_year_month     CHAR(6),
    trip_duration_minutes NUMERIC(10, 2),
    is_valid_trip         BOOLEAN     NOT NULL,
    is_incomplete_record  BOOLEAN     NOT NULL,
    invalid_reason        TEXT,

    -- Controle
    year                  SMALLINT    NOT NULL,
    month                 SMALLINT    NOT NULL,
    processed_at          TIMESTAMP   NOT NULL DEFAULT now()
);

COMMENT ON TABLE silver.slv_yellow_tripdata IS
    'Corridas tratadas e enriquecidas. Anomalias sinalizadas, nunca removidas. '
    'Chave de carga: (year, month), a competencia do arquivo de origem.';

COMMENT ON COLUMN silver.slv_yellow_tripdata.pickup_year_month IS
    'Competencia de negocio no formato yyyymm, derivada da data de embarque. '
    'Pode divergir de (year, month), que vem do arquivo. Nao usar como chave de carga.';
COMMENT ON COLUMN silver.slv_yellow_tripdata.is_valid_trip IS
    'Falso quando a corrida viola ao menos uma regra de qualidade. Nao trata pagamento.';
COMMENT ON COLUMN silver.slv_yellow_tripdata.is_incomplete_record IS
    'Verdadeiro quando o registro tem campo obrigatorio ausente. Hoje cobre passenger_count = 0. Nao afeta is_valid_trip.';
COMMENT ON COLUMN silver.slv_yellow_tripdata.invalid_reason IS
    'Todos os motivos violados, separados por virgula. Nulo quando a corrida e valida.';

CREATE INDEX IF NOT EXISTS idx_slv_yellow_tripdata_competencia
    ON silver.slv_yellow_tripdata (year, month);

CREATE INDEX IF NOT EXISTS idx_slv_yellow_tripdata_pickup_ym
    ON silver.slv_yellow_tripdata (pickup_year_month);
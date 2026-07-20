{{
    config(
        materialized='incremental',
        unique_key=['year', 'month'],
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- Fato de corridas. Grao: uma linha por corrida.
-- Materializacao incremental por competencia (year, month): reprocessar uma
-- competencia ja existente substitui as linhas dela (delete+insert), mantendo
-- a idempotencia do pipeline.
-- A fato guarda apenas chaves e medidas; os atributos descritivos vivem nas
-- dimensoes (modelagem estrela).

with silver as (

    select * from {{ source('silver', 'slv_yellow_tripdata') }}

    {% if is_incremental() %}
    -- Em execucao incremental, processa apenas a competencia recebida por
    -- variavel. A DAG passa --vars 'year: X, month: Y' e a fato reconstroi
    -- somente aquela competencia. O delete+insert cuida da substituicao.
    where year = {{ var('year') }} and month = {{ var('month') }}
    {% endif %}

),

final as (

    select
        -- Chaves estrangeiras para as dimensoes
        vendorid,
        payment_type,
        cast(to_char(pickup_date, 'YYYYMMDD') as integer) as pickup_date_id,

        -- Medidas
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        fare_amount,
        total_amount,

        -- Flags de qualidade e classificacao
        is_valid_trip,
        is_valid_payment,
        is_incomplete_record,
        invalid_reason,

        -- Competencia de negocio e de arquivo
        pickup_year_month,
        year,
        month

    from silver

)

select * from final
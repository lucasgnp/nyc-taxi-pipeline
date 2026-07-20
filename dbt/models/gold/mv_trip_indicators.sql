{{
    config(
        materialized='materialized_view'
    )
}}

-- Materialized view de indicadores (item 7).
-- Grao: vendorid x competencia de negocio (pickup_year_month).
-- Total de corridas considera todas (o volume operacional inclui as anomalias,
-- pois a corrida aconteceu). Valor, ticket e distancia consideram apenas
-- corridas validas (is_valid_trip), para nao serem distorcidos pelos estornos
-- e anomalias sinalizadas na silver.

select
    vendorid,
    pickup_year_month,

    -- Volume: todas as corridas, validas ou nao.
    count(*) as total_trips,

    -- Volume confiavel: apenas corridas validas. Permite calcular a taxa de
    -- invalidez por competencia sem recorrer a fato.
    count(*) filter (where is_valid_trip) as valid_trips,

    -- Valor total apenas das corridas validas.
    sum(total_amount) filter (where is_valid_trip) as total_valid_amount,

    -- Ticket medio: valor sobre as corridas validas.
    round(
        avg(total_amount) filter (where is_valid_trip), 
        2
    ) as avg_ticket,

    -- Distancia média sobre as corridas validas.
    round(
        avg(trip_distance) filter (where is_valid_trip),
        2
    ) as avg_distance

from {{ ref('fact_trips') }}
group by vendorid, pickup_year_month
having cast(pickup_year_month as integer) >= 202501
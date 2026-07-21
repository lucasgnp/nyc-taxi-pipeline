{{
    config(
        materialized='materialized_view'
    )
}}

-- Materialized view de indicadores (item 7).
-- Grão: vendorid x competencia de negocio (pickup_year_month).
-- Total de corridas considera todas (o volume operacional inclui as anomalias,
-- pois a corrida aconteceu). Os indicadores de receita consideram apenas
-- corridas validas E com pagamento valido, conforme o item 5 do desafio
-- ("considerar como receita valida apenas pagamentos validos"). O tipo de
-- pagamento 0 (Flex Fare) nao consta na tabela de referencia do desafio, entao
-- nao e considerado pagamento valido e fica fora da apuracao de receita.

select
    vendor_id,
    pickup_year_month,

    -- Volume: todas as corridas, validas ou nao.
    count(*) as total_trips,

    -- Volume de corridas que contam como receita valida: corrida sem anomalia
    -- e com pagamento valido.
    count(*) filter (where is_valid_trip and is_valid_payment) as valid_revenue_trips,

    -- Receita: soma apenas de corridas validas com pagamento valido.
    sum(total_amount) filter (where is_valid_trip and is_valid_payment) as total_valid_amount,

    -- Ticket medio sobre as corridas de receita valida.
    round(
        avg(total_amount) filter (where is_valid_trip and is_valid_payment), 
        2
    ) as avg_ticket,

    -- Distancia media sobre as corridas de receita valida.
    round(
        avg(trip_distance) filter (where is_valid_trip and is_valid_payment), 
        2
    ) as avg_distance

from {{ ref('fact_trips') }}
group by vendor_id, pickup_year_month
having cast(pickup_year_month as integer) >= 202501
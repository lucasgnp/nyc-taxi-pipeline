-- Dimensão de localização (zonas de táxi da NYC).
-- Fonte: tabela de referência da bronze (brz_taxi_zone_lookup).
-- Mapeia LocationID para bairro, zona e tipo de zona de serviço.
-- Inclui os códigos especiais 264 (Unknown) e 265 (Outside of NYC), presentes
-- no dado, para que o join da fato com pickup/dropoff nunca fique orfão.

with source as (

    select
        location_id,
        borough,
        zone,
        service_zone
    from {{ source('bronze', 'brz_taxi_zone_lookup') }}

)

select * from source
-- Dimensao de tipos de pagamento.
-- Fonte: tabela de referencia da bronze (brz_payment_type).
-- Acrescenta o tipo 0, que existe no dado real mas nao no dicionario da TLC.

with reference as (
    select
        payment_type,
        payment_description,
        is_valid_payment
    from {{ source('bronze', 'brz_payment_type') }}
),
/*
-- O payment_type = 0 aparece em 22,5% das corridas mas não consta no dicionário oficial. Trato como categoria explícita.
-- para que o join da fato nunca fique orfão e o segmento seja rastreável.
unknown_type as (
    select
        0 as payment_type,
        'Unknown (undocumented)' as payment_description,
        false as is_valid_payment
),
*/
final as (
    select * from reference
    --union all
    --select * from unknown_type
)

select * from final
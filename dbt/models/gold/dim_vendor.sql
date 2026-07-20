-- Dimensao de fornecedores de tecnologia (TPEP providers).
-- Fonte dos nomes: dicionario oficial de dados da NYC TLC (Yellow Taxi, mar/2025),
-- a mesma fonte citada no item 4 do desafio para os tipos de pagamento.
-- Os quatro codigos (1, 2, 6, 7) sao os observados no dado de 2025.

with vendors as (

    select 1 as vendorid, 'Creative Mobile Technologies, LLC' as vendor_name
    union all
    select 2, 'Curb Mobility, LLC'
    union all
    select 6, 'Myle Technologies Inc'
    union all
    select 7, 'Helix'

)

select
    vendorid as vendor_id,
    vendor_name
from vendors
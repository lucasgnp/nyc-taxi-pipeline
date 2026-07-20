-- Dimensao de motivos de invalidez.
-- Catalogo das regras de qualidade aplicadas na silver. Uma linha por regra.
-- O reason_code casa com os valores gravados em slv_yellow_tripdata.invalid_reason.
-- Inclui a linha 0 (corrida valida) para que o join da fato nunca fique orfao.

with reasons as (

    select 0 as reason_id, 'valid'                          as reason_code, 'Corrida valida (sem violacao)'          as reason_description
    union all
    select 1, 'duracao_acima_6h',            'Duracao acima de 6 horas'
    union all
    select 2, 'distancia_acima_100_milhas',  'Distancia acima de 100 milhas'
    union all
    select 3, 'duracao_nao_positiva',        'Duracao menor ou igual a zero'
    union all
    select 4, 'distancia_nao_positiva',      'Distancia menor ou igual a zero'
    union all
    select 5, 'valor_total_negativo',        'Valor total negativo'
    union all
    select 6, 'embarque_fora_da_competencia','Embarque fora da competencia do arquivo'

)

select
    reason_id,
    reason_code,
    reason_description
from reasons
-- Dimensao de data (calendario).
-- Uma linha por dia de 2020 a 2030, com atributos pre-calculados para analise
-- temporal. Intervalo fixo e generoso para cobrir backfill historico e anos
-- futuros sem manutenção, sustentando a atualização incremental do pipeline.

with date_spine as (

    select generate_series(
        '2020-01-01'::date,
        '2030-12-31'::date,
        interval '1 day'
    )::date as date_day

),

final as (

    select
        -- Chave da dimensao no formato yyyymmdd, inteiro. Ex: 20250115.
        -- Surrogate key legivel: da para ler a data na propria chave.
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_id,

        date_day,

        extract(year  from date_day)::int          as year,
        extract(month from date_day)::int          as month,
        extract(day   from date_day)::int          as day,
        extract(quarter from date_day)::int        as quarter,

        -- Competencia yyyymm, para casar com pickup_year_month da silver.
        to_char(date_day, 'YYYYMM')                as year_month,

        to_char(date_day, 'TMMonth')               as month_name,
        trim(to_char(date_day, 'TMDay'))           as day_name,

        -- extract(dow) devolve 0=domingo ... 6=sabado.
        extract(dow from date_day)::int            as day_of_week,
        (extract(dow from date_day) in (0, 6))     as is_weekend

    from date_spine

)

select * from final
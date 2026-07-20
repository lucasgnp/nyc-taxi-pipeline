{% macro generate_schema_name(custom_schema_name, node) -%}

    {#-
        Sobrescreve o comportamento padrao do dbt, que concatenaria o schema
        do profile com o do model (gerando gold_gold). Aqui uso o schema
        customizado diretamente quando ele existe, ou o do profile como fallback.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
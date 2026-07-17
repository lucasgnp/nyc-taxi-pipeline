-- Schemas da arquitetura medalhao no data warehouse.
-- Executado automaticamente pelo container do Postgres na primeira inicializacao.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA bronze IS 'Dados brutos, copia fiel da origem e tabelas de referencia. Sem transformacao.';
COMMENT ON SCHEMA silver IS 'Dados consolidados e tratados, com regras de qualidade aplicadas.';
COMMENT ON SCHEMA gold IS 'Modelagem dimensional e indicadores prontos para analise.';
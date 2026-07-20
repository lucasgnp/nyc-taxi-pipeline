-- Tabela de referência de zonas de táxi da NYC TLC.
-- Fonte: taxi_zone_lookup.csv (dicionário oficial da TLC).
-- Mapeia LocationID (usado em PULocationID/DOLocationID nas corridas) para
-- bairro, zona e tipo de zona de serviço.

CREATE TABLE IF NOT EXISTS bronze.brz_taxi_zone_lookup (
    location_id   INTEGER PRIMARY KEY,
    borough       TEXT,
    zone          TEXT,
    service_zone  TEXT
);

COMMENT ON TABLE bronze.brz_taxi_zone_lookup IS
    'Dominio de zonas de taxi da NYC. Referencia para PULocationID e DOLocationID.';
-- Tabela de referencia de tipos de pagamento.
-- Fonte: dicionario de dados do NYC TLC Trip Record Data.
-- Criada manualmente conforme o item 4 do desafio.

CREATE TABLE IF NOT EXISTS bronze.brz_payment_type (
    payment_type        INTEGER PRIMARY KEY,
    payment_description TEXT    NOT NULL,
    is_valid_payment    BOOLEAN NOT NULL
);

COMMENT ON TABLE bronze.brz_payment_type IS 'Dominio de tipos de pagamento e flag de validade para apuracao de receita.';

INSERT INTO bronze.brz_payment_type (payment_type, payment_description, is_valid_payment)
VALUES
    (1, 'Credit card', TRUE),
    (2, 'Cash',        TRUE),
    (3, 'No charge',   FALSE),
    (4, 'Dispute',     FALSE),
    (5, 'Unknown',     FALSE),
    (6, 'Voided trip', FALSE)
ON CONFLICT (payment_type) DO NOTHING;
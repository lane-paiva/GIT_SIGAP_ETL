CREATE OR REPLACE FUNCTION processa_sessoes_v5 (data_inicio TIMESTAMP, data_fim TIMESTAMP)
RETURNS TABLE (
    transaction_id BIGINT,
    csn_customer_id BIGINT,
    transaction_date TIMESTAMP,
    game_name TEXT,
    tipo_name TEXT,
    csn_game_id BIGINT,
    dif_data_atual_com_anterior DOUBLE PRECISION,
    anterior_game_id BIGINT,
    anterior_data TIMESTAMP,
    nova_sessao BOOLEAN,
    motivo_nova_sessao TEXT,
    id_sessao BIGINT
) AS $$
BEGIN
    RETURN QUERY
    WITH teste AS (
        SELECT
            ctv.customer_transaction_id::BIGINT AS transaction_id,
            ctv.csn_customer_id::BIGINT AS csn_customer_id,
            ctv.transaction_date::TIMESTAMP AS transaction_date,
            ctv.game_name::TEXT AS game_name,
            ctv.tipo_name::TEXT AS tipo_name,
            ctv.csn_game_id::BIGINT AS csn_game_id
        FROM customer_transactions_view ctv
        WHERE ctv.transaction_date IS NOT NULL
          AND ctv.csn_customer_id IS NOT NULL
          AND ctv.csn_game_id IS NOT NULL
          AND ctv.transaction_date BETWEEN data_inicio AND data_fim
        ORDER BY ctv.csn_customer_id, ctv.transaction_date ASC
    ),
    diferenca_sessao AS (
        SELECT
            tl.*,
            EXTRACT(EPOCH FROM (tl.transaction_date - LAG(tl.transaction_date)
                OVER (PARTITION BY tl.csn_customer_id ORDER BY tl.transaction_date)))::DOUBLE PRECISION AS dif_data_atual_com_anterior,
            LAG(tl.csn_game_id) OVER (PARTITION BY tl.csn_customer_id ORDER BY tl.transaction_date)::BIGINT AS anterior_game_id,
            LAG(tl.transaction_date) OVER (PARTITION BY tl.csn_customer_id ORDER BY tl.transaction_date)::TIMESTAMP AS anterior_data
        FROM teste tl
    ),
    base_sessao AS (
        SELECT
            ds.*,
            CASE
                WHEN ds.anterior_data IS NULL THEN TRUE
                WHEN ds.dif_data_atual_com_anterior > 1200 THEN TRUE
                WHEN ds.csn_game_id != ds.anterior_game_id THEN TRUE
                ELSE FALSE
            END AS nova_sessao,
            CASE
                WHEN ds.anterior_data IS NULL THEN 'Data anterior vazia'
                WHEN ds.dif_data_atual_com_anterior > 1200 AND ds.csn_game_id != ds.anterior_game_id THEN 'Passou 20 minutos, mas mudou de jogo'
                WHEN ds.dif_data_atual_com_anterior > 1200 THEN 'Passou 20 minutos'
                WHEN ds.csn_game_id != ds.anterior_game_id THEN 'Mudou de jogo'
                ELSE '-'
            END AS motivo_nova_sessao
        FROM diferenca_sessao ds
    )
    SELECT
        bs.transaction_id,
        bs.csn_customer_id,
        bs.transaction_date,
        bs.game_name,
        bs.tipo_name,
        bs.csn_game_id,
        bs.dif_data_atual_com_anterior,
        bs.anterior_game_id,
        bs.anterior_data,
        bs.nova_sessao,
        bs.motivo_nova_sessao,
        SUM(CASE WHEN bs.nova_sessao THEN 1 ELSE 0 END)
        OVER (ORDER BY bs.csn_customer_id, bs.transaction_date)::BIGINT AS id_sessao
    FROM base_sessao bs;
END;
$$ LANGUAGE plpgsql;


--como chama essa funcao
--SELECT * FROM processa_sessoes_v5 ( '2024-12-03 06:00:00', '2024-12-04 05:59:59');


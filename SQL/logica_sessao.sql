/********TESTE DE SESSAO**********/

DROP TABLE IF EXISTS teste_lane
DROP TABLE IF EXISTS diferenca_sessao
DROP TABLE IF EXISTS base_sessao
DROP TABLE IF EXISTS numeros_sessao
DROP TABLE IF EXISTS temp_tab_final

--campos essenciais nao nulos e ordem correta

select *
into temp teste_lane
from customer_transactions_view
where transaction_date is not null
      and csn_customer_id is not null
      and csn_game_id is not null
and transaction_date BETWEEN '2024-12-03 06:00:00' AND '2024-12-04 05:59:59'
--and csn_customer_id = '3867727'
order by csn_customer_id, transaction_date asc




--montando os campos da sessao
SELECT
        *,
        -- diferença de tempo entre transações em segundos
        EXTRACT(EPOCH FROM (transaction_date - LAG(transaction_date)
            OVER (PARTITION BY csn_customer_id ORDER BY transaction_date))) AS dif_data_atual_com_anterior,

        -- Identifica troca de jogo
        LAG(csn_game_id) OVER (PARTITION BY csn_customer_id ORDER BY transaction_date) AS anterior_game_id,
        LAG(transaction_date) OVER (PARTITION BY csn_customer_id ORDER BY transaction_date) anterior_data
into temp diferenca_sessao
from teste_lane


    SELECT
        *,
        -- Marca condição de nova sessão
        CASE
            WHEN anterior_data IS NULL THEN TRUE
            WHEN dif_data_atual_com_anterior > 1200 THEN TRUE
            WHEN csn_game_id != anterior_game_id THEN TRUE
            ELSE FALSE
        END AS nova_sessao,
       CASE
         WHEN anterior_data IS NULL THEN 'Data anterior vazia'
         WHEN dif_data_atual_com_anterior > 1200 AND csn_game_id != anterior_game_id THEN 'Passou 20 minutos, mas mudou de jogo'
         WHEN dif_data_atual_com_anterior > 1200 THEN 'Passou 20 minutos'
         WHEN csn_game_id != anterior_game_id THEN 'Mudou de jogo'
           ELSE '-'
      END AS motivo_nova_sessao
    into temp base_sessao
    FROM diferenca_sessao


--gera Ids únicos
SELECT *,
        -- Soma acumulada global para criar IDs únicos
        SUM(CASE WHEN nova_sessao THEN 1 ELSE 0 END)
        OVER (ORDER BY csn_customer_id, transaction_date) AS id_sessao
    into temp temp_tab_final
    FROM base_sessao


--select * from numeros_sessao where csn_customer_id = '3867727'

--DROP TABLE temp_tab_final



SELECT
    customer_transaction_id,
    csn_customer_id,
    csn_game_id,
    anterior_game_id,
    transaction_date,
    anterior_data,
    dif_data_atual_com_anterior,
    nova_sessao,
    id_sessao,
    motivo_nova_sessao
into tab_final_sessao_teste
FROM temp_tab_final -- tabela teste
--where csn_customer_id = '3867727'
ORDER BY transaction_date


--drop table tab_final_sessao_teste

--3867727
select * from tab_final_sessao_teste
where csn_customer_id = '3867727'


--chamada a funcao pra testar
SELECT *
FROM processa_sessoes_v5 ( '2024-12-03 06:00:00', '2024-12-04 05:59:59');
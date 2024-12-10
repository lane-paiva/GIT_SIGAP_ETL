import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Configurações do banco de dados de origem (sportingtech)
DATABASE_URI_ORIGEM = "postgresql://view_user:upbet123@sportingtech.upsports.app:5434/sporting_tech"
engine_origem = create_engine(DATABASE_URI_ORIGEM)

# Configurações do banco de dados de destino (sigapteste)
DATABASE_URI_DESTINO = 'postgresql://postgres:melo123@localhost:5432/sigapteste_2'
engine_destino = create_engine(DATABASE_URI_DESTINO)

# Nome da tabela no banco de dados de destino
table_name_casino = 'casino_customer_bets_filtered'

# Consulta para buscar os dados no banco de origem
query = """
SELECT 
    ccb.customer_id,
    ccb.csn_game_id,
    ccb.played_date,
    ccb.update_date,
    cg.vendor_game_name,
    cg.csn_category_id,
    cc.category_name AS csn_category_name
FROM casino_customer_bets ccb
JOIN csn_games cg ON ccb.csn_game_id = cg.csn_game_id
JOIN csn_categories cc ON cg.csn_category_id = cc.category_id
WHERE 
    ccb.played_date BETWEEN '2024-10-09 16:00:00' AND '2024-10-13 15:59:59';
"""

# Carregar os dados em chunks e salvar na tabela de destino
chunksize = 50000
try:
    # Carregar e inserir os dados
    for i, chunk in enumerate(pd.read_sql_query(query, engine_origem, chunksize=chunksize)):
        print(f"Carregando bloco {i + 1} no banco de dados de destino ({table_name_casino})...")
        chunk.to_sql(table_name_casino, engine_destino, if_exists='append', index=False)

    print(f"Dados carregados com sucesso na tabela '{table_name_casino}'.")
except Exception as e:
    print(f"Erro ao carregar os dados: {e}")

# Criar índices para otimizar as consultas
try:
    with psycopg2.connect(
        dbname="sigapteste_2", user="postgres", password="melo123", host="localhost", port="5432"
    ) as conn_destino:
        with conn_destino.cursor() as cursor:
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_customer_id ON {table_name_casino} (customer_id);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_played_date ON {table_name_casino} (played_date);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_update_date ON {table_name_casino} (update_date);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_csn_game_id ON {table_name_casino} (csn_game_id);")
            cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_vendor_game_name ON {table_name_casino} (vendor_game_name);")
            conn_destino.commit()
            print(f"Índices criados com sucesso para a tabela '{table_name_casino}'.")
except Exception as e:
    print(f"Erro ao criar índices: {e}")

# Executar os updates em transações_csn
try:
    with psycopg2.connect(
        dbname="sigapteste_2", user="postgres", password="melo123", host="localhost", port="5432"
    ) as conn_destino:
        with conn_destino.cursor() as cursor:
            # Primeiro UPDATE
            update_query_1 = """
            UPDATE transactions_csn t
            SET csn_game_id = c.csn_game_id
            FROM casino_customer_bets_filtered c
            WHERE t.csn_customer_id = c.customer_id
              AND t.transaction_date = c.played_date
              AND t.csn_cust_transaction_type_id = 1;
            """
            cursor.execute(update_query_1)
            conn_destino.commit()
            print("Primeiro UPDATE executado com sucesso.")

            # Segundo UPDATE (somente para campos NULL)
            update_query_2 = """
            UPDATE transactions_csn t
            SET csn_game_id = c.csn_game_id
            FROM casino_customer_bets_filtered c
            WHERE t.csn_customer_id = c.customer_id
              AND t.transaction_date = c.update_date
              AND t.csn_cust_transaction_type_id IN ('1', '2', '8', '15')
              AND t.csn_game_id IS NULL;
            """
            cursor.execute(update_query_2)
            conn_destino.commit()
            print("Segundo UPDATE executado com sucesso (apenas para campos NULL).")

            # Terceiro UPDATE (preencher usando o último valor não nulo)
            update_query_3 = """
            UPDATE transactions_csn t
            SET csn_game_id = sub.csn_game_id
            FROM (
                SELECT DISTINCT ON (t1.csn_customer_id, t1.csn_cust_transaction_type_id, t1.transaction_date)
                    t1.csn_customer_id,
                    t1.csn_cust_transaction_type_id,
                    t1.transaction_date,
                    t2.csn_game_id
                FROM transactions_csn t1
                JOIN transactions_csn t2
                ON t1.csn_customer_id = t2.csn_customer_id
                AND t1.csn_cust_transaction_type_id = t2.csn_cust_transaction_type_id
                AND t1.transaction_date > t2.transaction_date
                AND t2.csn_game_id IS NOT NULL
                WHERE t1.csn_game_id IS NULL
                AND t1.csn_cust_transaction_type_id IN ('1', '2', '8', '15') -- Alinhado com os tipos anteriores
                ORDER BY t1.csn_customer_id, t1.csn_cust_transaction_type_id, t1.transaction_date DESC
            ) sub
            WHERE t.csn_customer_id = sub.csn_customer_id
            AND t.csn_cust_transaction_type_id = sub.csn_cust_transaction_type_id
            AND t.transaction_date = sub.transaction_date
            AND t.csn_game_id IS NULL;
            """
            cursor.execute(update_query_3)
            conn_destino.commit()
            print("Terceiro UPDATE executado com sucesso (valores preenchidos com o último registro válido).")
except Exception as e:
    print(f"Erro ao executar os updates: {e}")

print("Script executado com sucesso.")

import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Configurações do banco local
local_config = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "melo123",
    "database": "sigapteste_2"
}

# Configurações do banco Sportingtech
sportingtech_config = {
    "host": "sportingtech.upsports.app",
    "port": 5434,
    "user": "view_user",
    "password": "upbet123",
    "database": "sporting_tech"
}

# Conexão com o banco
def connect_to_db(config):
    print(f"Conectando ao banco: {config['database']} em {config['host']}...")
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"]
    )

# Busca dados da tabela customer_transactions_csn
def fetch_transactions(cursor, start_date, end_date):
    print("Buscando transações no Sportingtech...")
    query = """
    SELECT customer_transaction_id, csn_customer_id, csn_cust_transaction_type_id,
           amount_from_balance, transaction_date, customer_code::BIGINT, current_balance
    FROM customer_transactions_csn
    WHERE transaction_date BETWEEN %s AND %s;
    """
    cursor.execute(query, (start_date, end_date))
    transactions = cursor.fetchall()
    print(f"Encontradas {len(transactions)} transações no intervalo.")
    return transactions

# Insere dados na tabela transactions_csn
def insert_transactions(cursor, data):
    print(f"Inserindo {len(data)} registros na tabela transactions_csn no banco local...")
    query = """
    INSERT INTO transactions_csn (
        customer_transaction_id, csn_customer_id, csn_cust_transaction_type_id,
        amount_from_balance, transaction_date, customer_code, current_balance
    ) VALUES %s;
    """
    execute_values(cursor, query, data)

# Cria índices na tabela transactions_csn
def create_indexes(cursor):
    print("Criando índices na tabela transactions_csn...")
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_transactions_csn_transaction_date ON transactions_csn (transaction_date);",
        "CREATE INDEX IF NOT EXISTS idx_transactions_csn_csn_customer_id ON transactions_csn (csn_customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_transactions_csn_csn_cust_transaction_type_id ON transactions_csn (csn_cust_transaction_type_id);"
    ]
    for query in indexes:
        cursor.execute(query)

# Script principal
def main():
    try:
        # Conecta aos bancos
        local_conn = connect_to_db(local_config)
        local_cursor = local_conn.cursor()
        sportingtech_conn = connect_to_db(sportingtech_config)
        sportingtech_cursor = sportingtech_conn.cursor()

        # Define o intervalo de datas
        start_date = datetime.strptime('2024-10-10 06:00:00', '%Y-%m-%d %H:%M:%S')
        end_date = datetime.strptime('2024-10-13 05:59:59', '%Y-%m-%d %H:%M:%S')

        # Busca transações no Sportingtech
        transactions = fetch_transactions(sportingtech_cursor, start_date, end_date)

        # Insere os dados no banco local
        if transactions:
            insert_transactions(local_cursor, transactions)
            create_indexes(local_cursor)  # Cria os índices
            local_conn.commit()
            print(f"{len(transactions)} registros inseridos com sucesso na tabela transactions_csn!")
        else:
            print("Nenhuma transação encontrada no intervalo.")

    except Exception as e:
        print(f"Erro: {e}")
    finally:
        # Fecha as conexões
        if 'local_cursor' in locals(): local_cursor.close()
        if 'local_conn' in locals(): local_conn.close()
        if 'sportingtech_cursor' in locals(): sportingtech_cursor.close()
        if 'sportingtech_conn' in locals(): sportingtech_conn.close()

if __name__ == "__main__":
    main()

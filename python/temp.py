import psycopg2
import os

# Obtenha os parâmetros passados pelo Pentaho (variáveis)
param_data_inicio = getVariable("data_inicio", "2024-01-01 00:00:00")  # Exemplo de variável
param_data_fim = getVariable("data_fim", "2024-12-31 23:59:59")  # Exemplo de variável

# Configurações de conexão com o banco PostgreSQL
host = "sportingtech.upsports.app"
dbname = "sporting_tech"
user = "view_user"
password = "upbet123"

# Conecte-se ao banco de dados PostgreSQL
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
cursor = conn.cursor()

# Crie a consulta para criar a tabela temporária
create_temp_table_query = """
CREATE TEMPORARY TABLE temp_customer_transactions AS
SELECT customer_transaction_id, 
       csn_customer_id, 
       csn_cust_transaction_type_id,
       amount_from_balance, 
       transaction_date, 
       customer_code::BIGINT, 
       current_balance
FROM customer_transactions_csn
WHERE transaction_date BETWEEN %s AND %s;
"""

# Execute a consulta para criar a tabela temporária
cursor.execute(create_temp_table_query, (param_data_inicio, param_data_fim))

# Confirme a transação
conn.commit()

# Agora, você pode usar um SELECT para pegar os resultados da tabela temporária
select_query = "SELECT * FROM temp_customer_transactions;"

cursor.execute(select_query)

# Obtenha os resultados
results = cursor.fetchall()

# Faça o que for necessário com os resultados
for row in results:
    print(row)  # Exemplo de exibição dos resultados

# Feche a conexão
cursor.close()
conn.close()

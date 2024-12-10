import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json

# Configurações do banco de dados
db_config = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "melo123",
    "database": "sigapteste_2"
}

# Configurações iniciais
valor_inicial_sessionid = 25888517
data_inicio = "2024-10-10 06:00:00"
data_fim = "2024-10-13 05:59:59"

# Conectar ao banco usando sqlalchemy
engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

print("Conectando ao banco e carregando dados...")

# Query para carregar os dados da tabela 'transactions_csn'
query = f"""
SELECT 
    customer_transaction_id, 
    csn_customer_id, 
    amount_from_balance, 
    transaction_date, 
    csn_game_id, 
    csn_cust_transaction_type_id, 
    current_balance
FROM transactions_csn
WHERE transaction_date BETWEEN '{data_inicio}' AND '{data_fim}';
"""

# Carregar os dados da tabela para um DataFrame
data = pd.read_sql_query(query, engine)
print(f"Dados carregados: {len(data)} linhas.")

# Converter transaction_date para datetime
data['transaction_date'] = pd.to_datetime(data['transaction_date'])

# Verificar dados essenciais
if data[['transaction_date', 'csn_customer_id', 'csn_game_id']].isnull().any().any():
    print("Aviso: Existem valores nulos nas colunas essenciais.")
    data.dropna(subset=['transaction_date', 'csn_customer_id', 'csn_game_id'], inplace=True)

# Ordenar os dados por csn_customer_id, transaction_date
data = data.sort_values(by=['csn_customer_id', 'transaction_date']).reset_index(drop=True)

# Recuperar o último sessionid salvo
try:
    last_sessionid = pd.read_sql_query("SELECT MAX(sessionid) as last_sessionid FROM session_summary_bate;", engine)['last_sessionid'].iloc[0]
    if pd.notnull(last_sessionid):
        valor_inicial_sessionid = last_sessionid + 1
except Exception as e:
    print("Erro ao recuperar último sessionid:", e)

# Calcular diferença de tempo entre transações
data['session_diff'] = data.groupby('csn_customer_id')['transaction_date'].diff().dt.total_seconds()

# Identificar novas sessões
data['new_session'] = (
    (data['session_diff'] > 1200) |  # Intervalo maior que 1200 segundos
    (data['csn_game_id'] != data['csn_game_id'].shift()) |  # Mudança no jogo
    (data['csn_customer_id'] != data['csn_customer_id'].shift())  # Mudança de cliente
)
data['new_session'] = data['new_session'].fillna(True)

# Gerar sessionid de forma única para cada nova sessão
data['sessionid'] = data['new_session'].cumsum() + valor_inicial_sessionid

# Criar colunas auxiliares para tipos de transação
data['is_aposta'] = data['csn_cust_transaction_type_id'] == 1
data['is_ganho'] = data['csn_cust_transaction_type_id'].isin([2, 8])
data['is_recompensa'] = data['csn_cust_transaction_type_id'] == 15
data['recompensa_paga'] = (data['is_recompensa'] & (data['amount_from_balance'] > 0)).astype(int)
data['recompensa_nao_paga'] = (data['is_recompensa'] & (data['amount_from_balance'] == 0)).astype(int)
data['total_apostas'] = data['amount_from_balance'].where(data['is_aposta'], 0)
data['total_ganhos'] = data['amount_from_balance'].where(data['is_ganho'], 0)
data['total_recompensas'] = data['amount_from_balance'].where(data['is_recompensa'], 0)

# Criar JSON com customer_transaction_id das recompensas pagas
data['customer_transaction_id'] = data['customer_transaction_id'].astype(str)  # Garantir string para JSON
data['json_recompensas'] = data['customer_transaction_id'].where(data['recompensa_paga'] == 1)

# Processar sessões
print("Calculando métricas por sessão...")
session_summary_bate = data.groupby(['csn_customer_id', 'sessionid', 'csn_game_id']).agg(
    quantidade_apostas=('is_aposta', 'sum'),
    quantidade_ganhos=('is_ganho', 'sum'),
    quantidade_recompensas_pagas=('recompensa_paga', 'sum'),
    quantidade_recompensas_nao_pagas=('recompensa_nao_paga', 'sum'),
    total_apostas=('total_apostas', 'sum'),
    total_ganhos=('total_ganhos', 'sum'),
    total_recompensas=('total_recompensas', 'sum'),
    session_start=('transaction_date', 'min'),
    session_end=('transaction_date', 'max'),
    quantidade_recompensas=('json_recompensas', lambda x: json.dumps(list(x.dropna())))
).reset_index()

# Calcular tempo da sessão, ganhos/perdas e resultado da sessão
session_summary_bate['session_duration_seconds'] = (
    (session_summary_bate['session_end'] - session_summary_bate['session_start']).dt.total_seconds()
)
session_summary_bate['ganhos_jogador'] = (session_summary_bate['total_ganhos'] - session_summary_bate['total_apostas']).clip(lower=0)
session_summary_bate['perda_jogador'] = (session_summary_bate['total_apostas'] - session_summary_bate['total_ganhos']).clip(lower=0)
session_summary_bate['result_session'] = session_summary_bate['total_ganhos'].apply(lambda x: 'ganho' if x > 0 else 'perda')

print("Métricas calculadas com sucesso.")

# Salvar os resultados no banco
print("Salvando os resultados no banco...")
session_summary_bate.to_sql('session_summary_bate', engine, if_exists='append', index=False)

# Fechar a conexão com o banco
engine.dispose()
print("Conexão com o banco fechada.")

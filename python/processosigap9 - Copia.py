import pandas as pd
from sqlalchemy import create_engine, text

# Configurações dos bancos de dados
db_config_analysis = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "melo123",
    "database": "sigapteste_2"
}

db_config_sportingtech = {
    "host": "sportingtech.upsports.app",
    "port": "5434",
    "user": "view_user",
    "password": "upbet123",
    "database": "sporting_tech"
}

# Conectar aos bancos
engine_analysis = create_engine(f"postgresql://{db_config_analysis['user']}:{db_config_analysis['password']}@{db_config_analysis['host']}:{db_config_analysis['port']}/{db_config_analysis['database']}")
engine_sportingtech = create_engine(f"postgresql://{db_config_sportingtech['user']}:{db_config_sportingtech['password']}@{db_config_sportingtech['host']}:{db_config_sportingtech['port']}/{db_config_sportingtech['database']}")

# Verificar se as colunas necessárias existem na tabela
required_columns = ['category', 'vendor_game_name']
existing_columns_query = """
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'session_summary_bate';
"""
existing_columns = pd.read_sql_query(existing_columns_query, engine_analysis)['column_name'].tolist()

missing_columns = [col for col in required_columns if col not in existing_columns]
if missing_columns:
    raise Exception(f"As colunas ausentes na tabela 'session_summary_bate' devem ser criadas: {missing_columns}")

# Carregar os `csn_game_id` únicos da tabela `session_summary_bate`
print("Carregando csn_game_id únicos da tabela 'session_summary_bate'...")
query_analysis = "SELECT DISTINCT csn_game_id FROM session_summary_bate WHERE csn_game_id IS NOT NULL;"
csn_game_ids = pd.read_sql_query(query_analysis, engine_analysis)

# Remover valores inválidos e duplicados
csn_game_ids = csn_game_ids.dropna().drop_duplicates()
if csn_game_ids.empty:
    print("Nenhum csn_game_id válido encontrado na tabela 'session_summary_bate'.")
else:
    print(f"Encontrados {len(csn_game_ids)} csn_game_id únicos.")

    # Transformar os IDs em uma tupla para uso no SQL
    csn_game_ids_list = tuple(csn_game_ids['csn_game_id'].astype(int).tolist())

    # Consultar informações no banco Sportingtech
    print("Consultando informações no banco 'Sportingtech'...")
    query_sportingtech = f"""
    SELECT DISTINCT
        games.csn_game_id AS csn_game_id,
        categories.category_name AS game_category,
        games.vendor_game_name AS vendor_name
    FROM
        csn_games games
    LEFT JOIN
        csn_categories categories ON games.csn_category_id = categories.category_id
    WHERE
        games.csn_game_id IN {csn_game_ids_list};
    """
    try:
        game_info = pd.read_sql_query(query_sportingtech, engine_sportingtech)

        # Verificar se há resultados
        if game_info.empty:
            print("Nenhuma informação encontrada para os csn_game_id fornecidos.")
        else:
            print(f"Encontradas {len(game_info)} informações correspondentes.")

            # Atualizar a tabela `session_summary_bate` com as informações
            print("Atualizando tabela 'session_summary_bate' com as informações...")
            with engine_analysis.begin() as connection:
                for _, row in game_info.iterrows():
                    print(f"Atualizando: csn_game_id={row['csn_game_id']}, category={row['game_category']}, vendor_name={row['vendor_name']}")
                    update_query = text("""
                    UPDATE session_summary_bate
                    SET category = :category,
                        vendor_game_name = :vendor_name
                    WHERE csn_game_id = :csn_game_id;
                    """)
                    connection.execute(update_query, {
                        'category': row['game_category'],
                        'vendor_name': row['vendor_name'],
                        'csn_game_id': int(row['csn_game_id'])
                    })

            print("Atualização concluída com sucesso.")

    except Exception as e:
        print(f"Erro ao consultar ou atualizar informações: {e}")

# Fechar conexões
engine_analysis.dispose()
engine_sportingtech.dispose()

print("Conexões com os bancos fechadas.")

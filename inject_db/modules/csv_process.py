import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table


def run():
    st.header("Processamento de Arquivo CSV")

    # Função para carregar o arquivo CSV e exibir colunas
    def load_csv(file):
        df = pd.read_csv(file)
        st.write("Visualização dos Dados:", df.head())
        return df

    # Função para se conectar ao banco de dados
    def connect_to_database(connection_string):
        engine = create_engine(connection_string)
        return engine

    # Função para listar as tabelas no banco de dados
    def list_tables(engine):
        inspector = inspect(engine)
        return inspector.get_table_names()

    # Função para listar colunas de uma tabela específica
    def list_columns(engine, table_name):
        inspector = inspect(engine)
        return [col['name'] for col in inspector.get_columns(table_name)]

    # Função para inserir dados na tabela
    def insert_data(engine, table_name, data):
        metadata = MetaData(bind=engine)
        table = Table(table_name, metadata, autoload_with=engine)
        conn = engine.connect()
        conn.execute(table.insert(), data.to_dict(orient='records'))
        conn.close()

    # Interface do usuário
    st.title("Inserção de Dados em Banco via CSV")
    file = st.file_uploader("Faça upload do seu arquivo CSV", type=["csv"])

    # Etapa de conexão com o banco
    db_url = st.text_input("Insira a URL de conexão com o banco de dados (ex: 'postgresql://user:password@host:port/dbname')")

    if db_url:
        engine = connect_to_database(db_url)
        st.success("Conectado ao banco de dados com sucesso!")
        
        if file:
            df = load_csv(file)
            
            # Listar tabelas e selecionar uma para inserir os dados
            tables = list_tables(engine)
            selected_table = st.selectbox("Selecione a tabela para inserir os dados:", tables)
            
            if selected_table:
                columns = list_columns(engine, selected_table)
                mappings = {}
                
                # Mapeamento de colunas
                st.subheader("Mapeamento de Colunas")
                for col in df.columns:
                    mapped_col = st.selectbox(f"Selecione a coluna correspondente para '{col}':", [None] + columns)
                    if mapped_col:
                        mappings[col] = mapped_col
                
                # Preparar os dados para inserção
                if st.button("Inserir dados"):
                    if mappings:
                        # Renomear as colunas do DataFrame de acordo com o mapeamento
                        df_mapped = df.rename(columns=mappings)
                        
                        # Selecionar apenas as colunas mapeadas para inserção
                        df_mapped = df_mapped[list(mappings.values())]
                        
                        # Inserir os dados na tabela
                        try:
                            insert_data(engine, selected_table, df_mapped)
                            st.success("Dados inseridos com sucesso!")
                        except Exception as e:
                            st.error(f"Erro ao inserir dados: {e}")
                    else:
                        st.warning("Por favor, mapeie todas as colunas antes de prosseguir.")

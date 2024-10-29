import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table

def run():
    st.header("Processamento de Arquivo CSV com Seleção Dinâmica")

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
    st.title("Inserção de Dados em Banco via CSV com Seleção Dinâmica")
    file = st.file_uploader("Faça upload do seu arquivo CSV", type=["csv"])

    # Etapa de conexão com o banco
    db_url = st.text_input("Insira a URL de conexão com o banco de dados (ex: 'postgresql://user:password@host:port/dbname')")

    if db_url:
        engine = connect_to_database(db_url)
        st.success("Conectado ao banco de dados com sucesso!")
        
        if file:
            df = load_csv(file)
            
            # Listar tabelas e colunas do banco para seleção
            tables = list_tables(engine)
            
            # Lista para armazenar os mapeamentos
            mappings = []

            st.subheader("Mapeamento de Colunas do CSV para o Banco de Dados")

            # Botão para adicionar uma nova linha de mapeamento
            if st.button("Adicionar Mapeamento"):
                mappings.append({"csv_column": None, "db_table": None, "db_column": None})

            # Mostrar todos os mapeamentos adicionados e permitir configuração
            for i, mapping in enumerate(mappings):
                # Colocar as seleções lado a lado para o mapeamento
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    col_csv = st.selectbox(f"Coluna CSV {i+1}", df.columns, key=f"csv_{i}")
                with col2:
                    table_db = st.selectbox(f"Tabela do Banco {i+1}", tables, key=f"table_{i}")
                with col3:
                    if table_db:
                        columns = list_columns(engine, table_db)
                        col_db = st.selectbox(f"Coluna do Banco {i+1}", columns, key=f"column_{i}")
                        
                        # Atualizar o mapeamento
                        mappings[i] = {"csv_column": col_csv, "db_table": table_db, "db_column": col_db}

                # Botão para remover este mapeamento, alinhado à direita
                st.markdown("<div style='text-align: right;'>", unsafe_allow_html=True)
                if st.button(f"Remover Mapeamento {i+1}"):
                    mappings.pop(i)
                st.markdown("</div>", unsafe_allow_html=True)

            # Botão para inserir dados no banco conforme os mapeamentos definidos
            if st.button("Inserir Dados"):
                if mappings:
                    # Iterar sobre os mapeamentos e inserir dados
                    for mapping in mappings:
                        csv_column = mapping["csv_column"]
                        db_table = mapping["db_table"]
                        db_column = mapping["db_column"]

                        if csv_column and db_table and db_column:
                            try:
                                # Preparar os dados para inserção
                                data_to_insert = df[[csv_column]].rename(columns={csv_column: db_column})
                                insert_data(engine, db_table, data_to_insert)
                                st.success(f"Dados de '{csv_column}' inseridos com sucesso na coluna '{db_column}' da tabela '{db_table}'!")
                            except Exception as e:
                                st.error(f"Erro ao inserir dados: {e}")
                        else:
                            st.warning("Por favor, complete todos os mapeamentos antes de prosseguir.")
                else:
                    st.warning("Por favor, adicione pelo menos um mapeamento.")

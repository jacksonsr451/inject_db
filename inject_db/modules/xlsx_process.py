import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table

def run():
    st.header("Processamento de Arquivo XLSX com Seleção Dinâmica")

    # Função para carregar o arquivo XLSX e exibir as colunas
    def load_excel(file):
        df = pd.read_excel(file)
        st.write("Visualização dos Dados:", df.head())
        return df

    # Função para se conectar ao banco de dados
    def connect_to_database(connection_string):
        engine = create_engine(connection_string)
        return engine

    # Função para listar tabelas no banco de dados
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
        with engine.connect() as conn:
            conn.execute(table.insert(), data.to_dict(orient='records'))

    # Interface do usuário
    st.title("Inserção de Dados em Banco via Excel com Seleção Dinâmica")
    file = st.file_uploader("Faça upload do seu arquivo Excel", type=["xlsx"])

    # Etapa de conexão com o banco
    db_url = st.text_input("Insira a URL de conexão com o banco de dados (ex: 'postgresql://user:password@host:port/dbname')")

    # Inicializa a lista de mapeamentos em `st.session_state`
    if "mappings" not in st.session_state:
        st.session_state["mappings"] = []

    if db_url:
        engine = connect_to_database(db_url)
        st.success("Conectado ao banco de dados com sucesso!")

        if file:
            df = load_excel(file)

            # Listar tabelas do banco para seleção
            tables = list_tables(engine)

            st.subheader("Mapeamento de Campos Excel para o Banco de Dados")

            # Botão para adicionar um novo mapeamento
            if st.button("Adicionar Mapeamento"):
                st.session_state["mappings"].append({"excel_field": None, "db_table": None, "db_column": None})

            # Exibir todos os mapeamentos adicionados e permitir configuração
            for i, mapping in enumerate(st.session_state["mappings"]):
                col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

                with col1:
                    excel_field = st.selectbox(f"Campo Excel {i+1}", df.columns, key=f"excel_{i}")
                with col2:
                    table_db = st.selectbox(f"Tabela do Banco {i+1}", tables, key=f"table_{i}")
                with col3:
                    if table_db:
                        columns = list_columns(engine, table_db)
                        db_column = st.selectbox(f"Coluna do Banco {i+1}", columns, key=f"column_{i}")
                        st.session_state["mappings"][i] = {"excel_field": excel_field, "db_table": table_db, "db_column": db_column}

                # Botão para remover este mapeamento
                with col4:
                    if st.button(f"Remover {i+1}", key=f"remove_{i}"):
                        st.session_state["mappings"].pop(i)
                        st.experimental_rerun()

            # Botão para inserir dados no banco conforme os mapeamentos definidos
            if st.button("Inserir Dados"):
                if st.session_state["mappings"]:
                    for mapping in st.session_state["mappings"]:
                        excel_field = mapping["excel_field"]
                        db_table = mapping["db_table"]
                        db_column = mapping["db_column"]

                        if excel_field and db_table and db_column:
                            try:
                                data_to_insert = df[[excel_field]].rename(columns={excel_field: db_column})
                                insert_data(engine, db_table, data_to_insert)
                                st.success(f"Dados de '{excel_field}' inseridos com sucesso na coluna '{db_column}' da tabela '{db_table}'!")
                            except Exception as e:
                                st.error(f"Erro ao inserir dados: {e}")
                        else:
                            st.warning("Por favor, complete todos os mapeamentos antes de prosseguir.")
                else:
                    st.warning("Por favor, adicione pelo menos um mapeamento.")


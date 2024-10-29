import json
import uuid

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

def run():
    def connect_db(dbname, user, password, host, port):
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(conn_str)
        return engine


    def get_tables(engine):
        query = (
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables = pd.read_sql(query, engine)
        return tables["table_name"].tolist()


    def get_columns(engine, table_name):
        query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
        columns = pd.read_sql(query, engine)
        return columns["column_name"].tolist()


    def generate_uuid():
        return str(uuid.uuid4())


    def fill_missing_uuids(data, id_column="id"):
        if id_column not in data.columns:
            data[id_column] = [generate_uuid() for _ in range(len(data))]
        else:
            data[id_column] = data[id_column].fillna(generate_uuid())
        return data


    def transfer_data(source_engine, dest_engine, query, table_dest, selected_columns):
        data = pd.read_sql(query, source_engine)

        data = fill_missing_uuids(data, id_column="id")

        for column in data.columns:
            if isinstance(data[column].iloc[0], dict):
                data[column] = data[column].apply(json.dumps)

        missing_cols = set(selected_columns) - set(get_columns(dest_engine, table_dest))
        if missing_cols:
            raise ValueError(
                f"As colunas a seguir estão ausentes na tabela de destino: {missing_cols}"
            )

        if "relationships" in st.session_state:
            for src_col, rel_dest_table, rel_dest_col in st.session_state["relationships"]:
                unique_values = data[src_col].unique()

                if len(unique_values) > 0:
                    rel_query = f"SELECT {rel_dest_col}, id FROM {rel_dest_table} WHERE {rel_dest_col} IN ({', '.join(map(str, unique_values))})"
                    rel_data = pd.read_sql(rel_query, dest_engine)

                    rel_map = dict(zip(rel_data[rel_dest_col], rel_data["id"]))

                    data[src_col] = data[src_col].map(rel_map)

        data.to_sql(table_dest, dest_engine, if_exists="append", index=False)
        return data


    st.title("Transferência de Dados entre Bancos de Dados PostgreSQL")

    st.header("Conexão com o Banco de Dados de Origem")
    dbname_src = st.text_input("Nome do banco de dados (Origem)")
    user_src = st.text_input("Usuário (Origem)")
    password_src = st.text_input("Senha (Origem)", type="password")
    host_src = st.text_input("Host (Origem)", value="localhost")
    port_src = st.text_input("Porta (Origem)", value="5432")

    if st.button("Conectar ao Banco de Origem"):
        try:
            source_engine = connect_db(
                dbname_src, user_src, password_src, host_src, port_src
            )
            st.session_state.source_engine = source_engine
            st.success("Conexão com o banco de dados de origem estabelecida com sucesso!")
        except Exception as e:
            st.error(f"Erro ao conectar ao banco de origem: {e}")

    st.header("Conexão com o Banco de Dados de Destino")
    dbname_dest = st.text_input("Nome do banco de dados (Destino)")
    user_dest = st.text_input("Usuário (Destino)")
    password_dest = st.text_input("Senha (Destino)", type="password")
    host_dest = st.text_input("Host (Destino)", value="localhost")
    port_dest = st.text_input("Porta (Destino)", value="5432")

    if st.button("Conectar ao Banco de Destino"):
        try:
            dest_engine = connect_db(
                dbname_dest, user_dest, password_dest, host_dest, port_dest
            )
            st.session_state.dest_engine = dest_engine
            st.success("Conexão com o banco de dados de destino estabelecida com sucesso!")
        except Exception as e:
            st.error(f"Erro ao conectar ao banco de dados de destino: {e}")

    if "source_engine" in st.session_state and "dest_engine" in st.session_state:
        st.header("Seleção de Tabelas e Colunas")

        tables_src = get_tables(st.session_state.source_engine)
        table_src = st.selectbox(
            "Selecione a tabela de origem", tables_src, key="source_table"
        )

        columns_src = get_columns(st.session_state.source_engine, table_src)
        selected_columns = st.multiselect(
            "Selecione as colunas de origem",
            columns_src,
            default=columns_src,
            key="source_columns",
        )

        query = f"SELECT {', '.join(selected_columns)} FROM {table_src}"

        tables_dest = get_tables(st.session_state.dest_engine)
        dest_table = st.selectbox(
            "Selecione a tabela de destino", tables_dest, key="dest_table"
        )

        columns_dest = get_columns(st.session_state.dest_engine, dest_table)
        dest_columns = st.multiselect(
            "Selecione as colunas da tabela de destino",
            columns_dest,
            default=columns_dest,
            key="dest_columns",
        )

        st.header("Definir Relacionamentos")
        activate_relationships = st.checkbox(
            "Ativar configuração de relacionamentos", value=False
        )

        if activate_relationships:
            rel_source_col = st.selectbox(
                "Selecione a coluna de origem para o relacionamento",
                columns_src,
                key="rel_source_col",
            )

            rel_dest_table = st.selectbox(
                "Selecione a tabela de destino para o relacionamento",
                tables_dest,
                key="rel_dest_table",
            )

            if rel_dest_table:
                rel_dest_columns = get_columns(st.session_state.dest_engine, rel_dest_table)

                rel_dest_col = st.selectbox(
                    "Selecione a coluna de destino para o relacionamento",
                    rel_dest_columns,
                    key="rel_dest_col",
                )
                st.write(f"Coluna de destino selecionada: {rel_dest_col}")

                if st.button("Adicionar Relacionamento"):
                    if "relationships" not in st.session_state:
                        st.session_state["relationships"] = []
                    st.session_state["relationships"].append(
                        (rel_source_col, rel_dest_table, rel_dest_col)
                    )
                    st.success("Relacionamento adicionado com sucesso!")

                if (
                    "relationships" in st.session_state
                    and st.session_state["relationships"]
                ):
                    st.header("Relacionamentos Adicionados")

                    for i, (src_col, dest_table, dest_col) in enumerate(
                        st.session_state["relationships"]
                    ):
                        st.write(f"Relacionamento {i + 1}:")
                        st.write(f"Coluna de origem: {src_col}")
                        st.write(f"Tabela de destino: {dest_table}")
                        st.write(f"Coluna de destino: {dest_col}")

                        if st.button(
                            f"Remover Relacionamento {i + 1}",
                            key=f"remove_relationship_{i}",
                        ):
                            st.session_state["relationships"].pop(i)
                            st.success(f"Relacionamento {i + 1} removido com sucesso!")
                            st.experimental_rerun()
                else:
                    st.write("Nenhum relacionamento adicionado.")

        if st.button("Transferir Dados"):
            try:
                transfer_data(
                    st.session_state.source_engine,
                    st.session_state.dest_engine,
                    query,
                    dest_table,
                    selected_columns,
                )
                st.success("Dados transferidos com sucesso!")
            except Exception as e:
                st.error(f"Erro ao transferir dados: {e}")
    else:
        st.warning(
            "Conecte-se aos bancos de dados de origem e destino antes de selecionar tabelas e colunas."
        )

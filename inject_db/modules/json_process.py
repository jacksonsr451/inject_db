import uuid

import pandas as pd
import streamlit as st
from sqlalchemy import MetaData, Table, create_engine, inspect


# Função para carregar o arquivo JSON e exibir colunas
def load_json(file):
    df = pd.read_json(file, lines=True)
    st.write('Visualização dos Dados:', df.head())
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


# Função para inserir dados na tabela com UUID4
def insert_data_with_uuid(engine, table_name, data):
    data['id'] = [str(uuid.uuid4()) for _ in range(len(data))]

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    conn = engine.connect()
    conn.execute(table.insert(), data.to_dict(orient='records'))
    conn.close()


def run():
    st.header(
        'Processamento de Arquivo JSON com Seleção Dinâmica e Relacionamentos'
    )

    # Interface do usuário
    st.title('Inserção de Dados em Banco via JSON com Seleção Dinâmica')

    file = st.file_uploader('Faça upload do seu arquivo JSON', type=['json'])
    db_url = st.text_input(
        "Insira a URL de conexão com o banco de dados (ex: 'postgresql://user:password@host:port/dbname')"
    )

    if db_url:
        engine = connect_to_database(db_url)
        st.success('Conectado ao banco de dados com sucesso!')

        if file:
            df = load_json(file)
            tables = list_tables(engine)

            # Listas para armazenar mapeamentos e relacionamentos
            mappings = []
            relationships = []

            st.subheader('Mapeamento de Campos JSON para o Banco de Dados')

            # Botão para adicionar um novo mapeamento
            if st.button('Adicionar Mapeamento'):
                mappings.append(
                    {'json_field': None, 'db_table': None, 'db_column': None}
                )

            # Exibir os mapeamentos adicionados
            for i, mapping in enumerate(mappings):
                col1, col2, col3 = st.columns(3)

                with col1:
                    json_field = st.selectbox(
                        f'Campo JSON {i+1}', df.columns, key=f'json_{i}'
                    )
                with col2:
                    table_db = st.selectbox(
                        f'Tabela do Banco {i+1}', tables, key=f'table_{i}'
                    )
                with col3:
                    if table_db:
                        columns = list_columns(engine, table_db)
                        db_column = st.selectbox(
                            f'Coluna do Banco {i+1}',
                            columns,
                            key=f'column_{i}',
                        )

                        mappings[i] = {
                            'json_field': json_field,
                            'db_table': table_db,
                            'db_column': db_column,
                        }

                # Botão para remover mapeamento
                st.markdown(
                    "<div style='text-align: right;'>", unsafe_allow_html=True
                )
                if st.button(f'Remover Mapeamento {i+1}'):
                    mappings.pop(i)
                st.markdown('</div>', unsafe_allow_html=True)

            # Controle de visibilidade da interface de relacionamento
            add_relationship = st.checkbox('Adicionar Relacionamento')

            if add_relationship:
                st.subheader('Configurar Relacionamentos entre Tabelas')

                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    table_origin = st.selectbox(
                        'Tabela de Origem', tables, key='rel_origin_table'
                    )
                with col2:
                    if table_origin:
                        columns_origin = list_columns(engine, table_origin)
                        column_origin = st.selectbox(
                            'Coluna de Origem',
                            columns_origin,
                            key='rel_origin_column',
                        )
                with col3:
                    table_dest = st.selectbox(
                        'Tabela de Destino', tables, key='rel_dest_table'
                    )
                with col4:
                    if table_dest:
                        columns_dest = list_columns(engine, table_dest)
                        column_dest = st.selectbox(
                            'Coluna de Destino',
                            columns_dest,
                            key='rel_dest_column',
                        )

                if st.button('Confirmar Relacionamento'):
                    if (
                        table_origin
                        and column_origin
                        and table_dest
                        and column_dest
                    ):
                        relationships.append(
                            {
                                'table_origin': table_origin,
                                'column_origin': column_origin,
                                'table_dest': table_dest,
                                'column_dest': column_dest,
                            }
                        )
                        st.success(
                            f'Relacionamento adicionado: {table_origin} ({column_origin}) -> {table_dest} ({column_dest})'
                        )

                if st.button('Remover Último Relacionamento'):
                    if relationships:
                        removed_relationship = relationships.pop()
                        st.warning(
                            f"Relacionamento removido: {removed_relationship['table_origin']} ({removed_relationship['column_origin']}) -> {removed_relationship['table_dest']} ({removed_relationship['column_dest']})"
                        )
                    else:
                        st.warning('Nenhum relacionamento para remover.')

            # Inserção de dados com base nos mapeamentos e relacionamentos
            if st.button('Inserir Dados'):
                if mappings:
                    for mapping in mappings:
                        json_field = mapping['json_field']
                        db_table = mapping['db_table']
                        db_column = mapping['db_column']

                        if json_field and db_table and db_column:
                            try:
                                data_to_insert = df[[json_field]].rename(
                                    columns={json_field: db_column}
                                )

                                for relationship in relationships:
                                    if (
                                        relationship['table_origin']
                                        == db_table
                                        and relationship['column_origin']
                                        in data_to_insert.columns
                                    ):
                                        data_to_insert[
                                            relationship['column_dest']
                                        ] = data_to_insert[
                                            relationship['column_origin']
                                        ]

                                insert_data_with_uuid(
                                    engine, db_table, data_to_insert
                                )
                                st.success(
                                    f"Dados de '{json_field}' inseridos com sucesso na coluna '{db_column}' da tabela '{db_table}'!"
                                )
                            except Exception as e:
                                st.error(f'Erro ao inserir dados: {e}')
                        else:
                            st.warning(
                                'Por favor, complete todos os mapeamentos antes de prosseguir.'
                            )
                else:
                    st.warning('Por favor, adicione pelo menos um mapeamento.')

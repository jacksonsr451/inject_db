import os
import sys
import unittest
import warnings
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import streamlit as st

from inject_db.modules.ods_process import (
    connect_to_database,
    insert_data,
    list_columns,
    list_tables,
    load_ods,
)


class TestODSProcess(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Redireciona stderr para evitar mensagens indesejadas
        cls.original_stderr = sys.stderr
        sys.stderr = open(os.devnull, 'w')
        # Ignora avisos específicos do Streamlit
        warnings.filterwarnings(
            'ignore', category=UserWarning, module='streamlit'
        )
        warnings.filterwarnings('ignore', category=Warning)

    @classmethod
    def tearDownClass(cls):
        # Restaura stderr
        sys.stderr.close()
        sys.stderr = cls.original_stderr

    @patch('inject_db.modules.ods_process.pd.read_excel')
    def test_load_ods(self, mock_read_excel):
        # Cria um DataFrame de exemplo para o teste
        mock_df = pd.DataFrame(
            {'col1': ['value1', 'value2'], 'col2': ['value3', 'value4']}
        )
        mock_read_excel.return_value = mock_df

        file = BytesIO()  # Simula um arquivo em memória
        result_df = load_ods(file)

        # Verifica se a função leu o arquivo corretamente
        mock_read_excel.assert_called_once_with(file, engine='odf')
        pd.testing.assert_frame_equal(result_df, mock_df)

    @patch('inject_db.modules.ods_process.create_engine')
    def test_connect_to_database(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        connection_string = 'sqlite:///:memory:'

        engine = connect_to_database(connection_string)

        # Verifica se a função create_engine foi chamada corretamente
        mock_create_engine.assert_called_once_with(connection_string)
        self.assertEqual(engine, mock_engine)

    @patch('inject_db.modules.ods_process.inspect')
    def test_list_tables(self, mock_inspect):
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_table_names.return_value = ['table1', 'table2']
        mock_engine = MagicMock()

        tables = list_tables(mock_engine)

        # Verifica se as tabelas são listadas corretamente
        self.assertEqual(tables, ['table1', 'table2'])
        mock_inspect.assert_called_once_with(mock_engine)
        mock_inspector.get_table_names.assert_called_once()

    @patch('inject_db.modules.ods_process.inspect')
    def test_list_columns(self, mock_inspect):
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.get_columns.return_value = [
            {'name': 'col1'},
            {'name': 'col2'},
        ]
        mock_engine = MagicMock()

        columns = list_columns(mock_engine, 'table1')

        # Verifica se as colunas são listadas corretamente
        self.assertEqual(columns, ['col1', 'col2'])
        mock_inspect.assert_called_once_with(mock_engine)
        mock_inspector.get_columns.assert_called_once_with('table1')

    @patch('inject_db.modules.ods_process.Table')
    @patch('inject_db.modules.ods_process.MetaData')
    def test_insert_data(self, mock_metadata, mock_table):
        mock_table_instance = MagicMock()
        mock_table.return_value = mock_table_instance
        mock_engine = MagicMock()

        # Simula o comportamento do método connect
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = (
            mock_connection
        )

        data = pd.DataFrame({'db_column': ['data1', 'data2', 'data3']})

        insert_data(mock_engine, 'mock_table', data)

        # Verifica se a inserção foi chamada corretamente
        mock_table.assert_called_once_with(
            'mock_table', mock_metadata(), autoload_with=mock_engine
        )
        mock_connection.execute.assert_called_once_with(
            mock_table_instance.insert(),
            data.to_dict(orient='records'),
        )


if __name__ == '__main__':
    unittest.main()

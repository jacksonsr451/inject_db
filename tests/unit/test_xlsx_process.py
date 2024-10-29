import os
import sys
import unittest
import warnings
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import streamlit as st

from inject_db.modules.xlsx_process import (
    connect_to_database,
    insert_data,
    list_columns,
    list_tables,
    load_excel,
)


class TestExcelToDatabase(unittest.TestCase):
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

    def test_load_excel(self):
        # Criar um arquivo Excel em memória
        excel_data = BytesIO()
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        df.to_excel(excel_data, index=False)
        excel_data.seek(0)  # Resetar o ponteiro do arquivo

        # Testar a função load_excel
        with patch('streamlit.write') as mock_write:
            result_df = load_excel(excel_data)
            # Comparar a estrutura do DataFrame e não o objeto em si
            mock_write.assert_called_once()
            self.assertTrue(
                mock_write.call_args[0][0].startswith(
                    'Visualização dos Dados:'
                )
            )
            pd.testing.assert_frame_equal(result_df, df)

    @patch('inject_db.modules.xlsx_process.create_engine')
    def test_connect_to_database(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        connection_string = 'sqlite:///:memory:'

        engine = connect_to_database(connection_string)

        # Verifica se a função create_engine foi chamada corretamente
        mock_create_engine.assert_called_once_with(connection_string)
        self.assertEqual(engine, mock_engine)

    @patch('inject_db.modules.xlsx_process.inspect')
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

    @patch('inject_db.modules.xlsx_process.inspect')
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

    @patch('inject_db.modules.xlsx_process.Table')
    @patch('inject_db.modules.xlsx_process.MetaData')
    def test_insert_data(self, mock_metadata, mock_table):
        mock_table_instance = MagicMock()
        mock_table.return_value = mock_table_instance
        mock_engine = MagicMock()

        # Simular o contexto de conexão
        mock_connection = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = (
            mock_connection
        )

        data = pd.DataFrame({'col1': [1, 2]})
        table_name = 'mock_table'

        insert_data(mock_engine, table_name, data)

        # Verifica se os dados foram inseridos corretamente
        mock_table_instance.insert.assert_called_once()
        mock_connection.execute.assert_called_once_with(
            mock_table_instance.insert(),
            data.to_dict(orient='records'),
        )


if __name__ == '__main__':
    unittest.main()

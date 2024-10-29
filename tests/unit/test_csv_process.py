import os
import sys
import unittest
import warnings
from io import StringIO
from unittest.mock import MagicMock, patch

import pandas as pd

from inject_db.modules.csv_process import (
    connect_to_database,
    insert_data_with_uuid,
    list_columns,
    list_tables,
    load_csv,
)


class TestCSVToDatabase(unittest.TestCase):
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

    @patch('inject_db.modules.csv_process.create_engine')
    def test_connect_to_database(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        db_url = 'sqlite:///:memory:'

        engine = connect_to_database(db_url)

        # Verifica se a função create_engine foi chamada corretamente
        mock_create_engine.assert_called_once_with(db_url)
        self.assertEqual(engine, mock_engine)

    @patch('inject_db.modules.csv_process.inspect')
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

    @patch('inject_db.modules.csv_process.inspect')
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

    @patch('inject_db.modules.csv_process.uuid.uuid4')
    @patch('inject_db.modules.csv_process.Table')
    def test_insert_data_with_uuid(self, mock_table, mock_uuid4):
        mock_uuid4.side_effect = ['uuid1', 'uuid2', 'uuid3']
        mock_table_instance = MagicMock()
        mock_table.return_value = mock_table_instance
        mock_engine = MagicMock()

        data = pd.DataFrame({'csv_column': ['data1', 'data2', 'data3']})

        insert_data_with_uuid(mock_engine, 'mock_table', data)

        # Verifica se o UUID foi adicionado e inserido corretamente
        data_with_uuid = data.copy()
        data_with_uuid['id'] = ['uuid1', 'uuid2', 'uuid3']
        mock_table_instance.insert.assert_called_once()
        mock_engine.connect().execute.assert_called_once_with(
            mock_table_instance.insert(),
            data_with_uuid.to_dict(orient='records'),
        )

    @patch('inject_db.modules.csv_process.pd.read_csv')
    def test_load_csv(self, mock_read_csv):
        csv_content = StringIO('col1,col2\nvalue1,value2\nvalue3,value4')
        mock_df = pd.DataFrame(
            {'col1': ['value1', 'value3'], 'col2': ['value2', 'value4']}
        )
        mock_read_csv.return_value = mock_df

        result_df = load_csv(csv_content)

        # Verifica se o CSV foi lido corretamente
        mock_read_csv.assert_called_once_with(csv_content)
        pd.testing.assert_frame_equal(result_df, mock_df)


if __name__ == '__main__':
    unittest.main()

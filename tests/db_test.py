import unittest
from unittest.mock import Mock, patch
from equalexperts_dataeng_exercise.db import (
    get_connection,
    setup_schema_and_table,
    WAREHOUSE_PATH,
    SCHEMA_NAME,
    MAIN_TABLE_NAME
)


class TestGetConnection(unittest.TestCase):

    @patch('equalexperts_dataeng_exercise.db.duckdb.connect')
    def test_get_connection_calls_duckdb_connect_with_correct_path(self, mock_connect):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        result = get_connection()
        
        mock_connect.assert_called_once_with(WAREHOUSE_PATH)
        assert result == mock_conn


class TestSetupSchemaAndTable(unittest.TestCase):

    def test_setup_schema_and_table_executes_sql_with_correct_schema_and_table_names(self):
        mock_conn = Mock()
        
        setup_schema_and_table(mock_conn)
        
        mock_conn.sql.assert_called_once()
        sql_call = mock_conn.sql.call_args[0][0]
        
        assert SCHEMA_NAME in sql_call
        assert MAIN_TABLE_NAME in sql_call
        
    def test_setup_schema_and_table_sql_contains_correct_table_structure(self):
        mock_conn = Mock()
        
        setup_schema_and_table(mock_conn)
        
        sql_call = mock_conn.sql.call_args[0][0]
        
        assert "id STRING NOT NULL PRIMARY KEY" in sql_call
        assert "user_id STRING" in sql_call
        assert "post_id STRING NOT NULL" in sql_call
        assert "vote_type_id INTEGER" in sql_call
        assert "bounty_amount DOUBLE" in sql_call
        assert "creation_date TIMESTAMP NOT NULL" in sql_call
    
    def test_setup_schema_and_table_uses_if_not_exists_clauses(self):
        mock_conn = Mock()
        
        setup_schema_and_table(mock_conn)
        
        sql_call = mock_conn.sql.call_args[0][0]
        assert "CREATE SCHEMA IF NOT EXISTS" in sql_call
        assert "CREATE TABLE IF NOT EXISTS" in sql_call

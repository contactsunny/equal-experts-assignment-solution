import pytest
import unittest
import os
import subprocess
import json
import duckdb
from unittest.mock import Mock, patch, mock_open
from equalexperts_dataeng_exercise.ingest import (
    validate_arguments, 
    validate_file_path, 
    validate_file_has_required_columns,
    update_main_table_from_stage_table,
    drop_stage_table,
    ingest_data,
    STAGE_TABLE_NAME
)
from equalexperts_dataeng_exercise.db import SCHEMA_NAME, MAIN_TABLE_NAME


class TestValidateArguments(unittest.TestCase):

    def test_validate_arguments_with_correct_count_passes(self):
        args = ["script_name", "file_path"]
        # Should pass without errors
        validate_arguments(args)
    
    def test_validate_arguments_with_incorrect_count_raises_value_error(self):
        missing_arguments = ["script_name"]
        extra_arguments = ["script_name", "file_path", "extra_arg"]
        
        with self.assertRaises(ValueError) as context:
            validate_arguments(missing_arguments)
        assert "Usage: python ingest.py <file_path>" in str(context.exception)
        
        with self.assertRaises(ValueError) as context:
            validate_arguments(extra_arguments)
        assert "Usage: python ingest.py <file_path>" in str(context.exception)


class TestValidateFilePath(unittest.TestCase):

    @patch('equalexperts_dataeng_exercise.ingest.os.path.exists')
    def test_validate_file_path_with_existing_file_passes(self, mock_exists):
        mock_exists.return_value = True
        file_path = "test_file.json"
        
        validate_file_path(file_path)
        mock_exists.assert_called_once_with(file_path)
    
    @patch('equalexperts_dataeng_exercise.ingest.os.path.exists')
    def test_validate_file_path_with_nonexistent_file_raises_file_not_found_error(self, mock_exists):
        mock_exists.return_value = False
        file_path = "nonexistent_file.json"
        
        with self.assertRaises(FileNotFoundError) as context:
            validate_file_path(file_path)
        assert f"File {file_path} does not exist" in str(context.exception)
        mock_exists.assert_called_once_with(file_path)


class TestValidateFileHasRequiredColumns(unittest.TestCase):

    def test_validate_file_has_required_columns_with_all_required_columns_returns_true(self):
        test_data = {
            "Id": "1",
            "PostId": "post1",
            "VoteTypeId": "2",
            "CreationDate": "2022-01-01T00:00:00.000"
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(test_data))):
            result = validate_file_has_required_columns("test_file.json")
            assert result is True
    
    def test_validate_file_has_required_columns_with_empty_file_returns_false(self):
        with patch('builtins.open', mock_open(read_data="")):
            result = validate_file_has_required_columns("empty_file.json")
            assert result is False
    
    def test_validate_file_has_required_columns_with_invalid_json_raises_exception(self):
        with patch('builtins.open', mock_open(read_data="invalid json content")):
            with self.assertRaises(json.JSONDecodeError):
                validate_file_has_required_columns("invalid_file.json")
    
    def test_validate_file_has_required_columns_with_extra_columns_returns_true(self):
        test_data = {
            "Id": "1",
            "UserId": "user1",
            "PostId": "post1", 
            "VoteTypeId": "2",
            "BountyAmount": "10.5",
            "CreationDate": "2022-01-01T00:00:00.000",
            "ExtraField": "extra_value",
            "AnotherField": "another_value"
        }
        
        with patch('builtins.open', mock_open(read_data=json.dumps(test_data))):
            result = validate_file_has_required_columns("test_file.json")
            assert result is True


class TestUpdateMainTableFromStageTable(unittest.TestCase):

    def test_update_main_table_from_stage_table_executes_correct_sql(self):
        mock_conn = Mock()
        update_main_table_from_stage_table(mock_conn)
        
        mock_conn.execute.assert_called_once()
        sql_call = mock_conn.execute.call_args[0][0]
        
        assert f"INSERT OR REPLACE INTO {SCHEMA_NAME}.{MAIN_TABLE_NAME}" in sql_call
        assert f"FROM {SCHEMA_NAME}.{STAGE_TABLE_NAME}" in sql_call
        assert "id, user_id, post_id, vote_type_id, bounty_amount, creation_date" in sql_call


class TestDropStageTable(unittest.TestCase):

    def test_drop_stage_table_executes_correct_sql(self):
        mock_conn = Mock()
        drop_stage_table(mock_conn)
        
        mock_conn.execute.assert_called_once()
        sql_call = mock_conn.execute.call_args[0][0]
        
        assert f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{STAGE_TABLE_NAME}" in sql_call


class TestIngestData(unittest.TestCase):

    @patch('equalexperts_dataeng_exercise.ingest.validate_file_has_required_columns')
    def test_ingest_data_raises_error_when_validation_fails(self, mock_validate):
        mock_conn = Mock()
        mock_validate.return_value = False
        file_path = "invalid_data.json"
        
        with self.assertRaises(ValueError) as context:
            ingest_data(file_path, mock_conn)
        
        assert f"File {file_path} does not have required columns for ingestion" in str(context.exception)
        mock_validate.assert_called_once_with(file_path)
        mock_conn.execute.assert_not_called()


def _count_unique_rows_in_data_file(file_path):
    ids = set()
    with open(file_path, "r", encoding="utf-8") as data:
        for line in data:
            ids.add(json.loads(line)["Id"])
    return len(ids)


class TestIngestionIntegration(unittest.TestCase):

    def setUp(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")
    
    def tearDown(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")
    
    def test_ingestion_failure_on_missing_column_in_file(self):
        file_path = "tests/test-resources/samples-votes-with-missing-fields.jsonl"
        if not os.path.exists(file_path):
            self.skipTest(f"Test file {file_path} not found")
            
        result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.ingest",
                file_path,
            ],
            capture_output=True,
        )

        assert result.returncode != 0
    
    def test_validate_file_has_required_columns_using_real_file(self):
        file_path = "tests/test-resources/samples-votes-with-duplicates.jsonl"
        if not os.path.exists(file_path):
            self.skipTest(f"Test file {file_path} not found")
        
        result = validate_file_has_required_columns(file_path)
        assert result is True

    def test_ingestion_completion_with_valid_sample_data(self):
        file_path = "tests/test-resources/samples-votes.jsonl"
        if not os.path.exists(file_path):
            self.skipTest(f"Test file {file_path} not found")
        
        if not validate_file_has_required_columns(file_path):
            self.skipTest(f"Test file {file_path} does not have required columns for ingestion")
            
        result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.ingest",
                file_path,
            ],
            capture_output=True,
        )

        assert result.returncode == 0

    def test_ingestion_handles_duplicate_records(self):
        file_path = "tests/test-resources/samples-votes-with-duplicates.jsonl"
        if not os.path.exists(file_path):
            self.skipTest(f"Test file {file_path} not found")
        
        if not validate_file_has_required_columns(file_path):
            self.skipTest(f"Test file {file_path} does not have required columns for ingestion")
            
        result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.ingest",
                file_path,
            ],
            capture_output=True,
        )
        
        assert result.returncode == 0

        if os.path.exists("warehouse.db"):
            sql = """
                SELECT COUNT(*) FROM blog_analysis.votes;
            """
            conn = duckdb.connect("warehouse.db", read_only=True)
            query_result = conn.sql(sql)
            expected_count = _count_unique_rows_in_data_file(file_path)
            actual_count = query_result.fetchall()[0][0]
            assert actual_count == expected_count
            conn.close()
        else:
            self.fail("Database file was not created during ingestion")

    def test_ingestion_fails_when_file_not_exists(self):
        result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.ingest",
                "unknown-file.jsonl"
            ],
            capture_output=True,
        )

        assert result.returncode != 0


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")
    yield
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")

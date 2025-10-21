import pytest
import subprocess
import duckdb
import os
import unittest
from unittest.mock import Mock
from equalexperts_dataeng_exercise.outliers import (
    create_outliers_view,
    OUTLIER_WEEKS_VIEW_NAME, 
    OUTLIER_THRESHOLD, 
    WEEK_NUMBER_MODULO,
    SCHEMA_NAME,
    MAIN_TABLE_NAME
)
from equalexperts_dataeng_exercise.db import get_connection, setup_schema_and_table


class TestCreateOutliersView(unittest.TestCase):

    def test_create_outliers_view_executes_sql_with_correct_structure(self):
        mock_conn = Mock()
        create_outliers_view(mock_conn)
        
        mock_conn.execute.assert_called_once()
        sql_call = mock_conn.execute.call_args[0][0]
        
        assert f"CREATE OR REPLACE VIEW {SCHEMA_NAME}.{OUTLIER_WEEKS_VIEW_NAME}" in sql_call
        assert f"FROM {SCHEMA_NAME}.{MAIN_TABLE_NAME}" in sql_call
        assert "EXTRACT(YEAR FROM creation_date) AS year" in sql_call
        assert f"EXTRACT(WEEK FROM creation_date) % {WEEK_NUMBER_MODULO} AS week_number" in sql_call
        assert "COUNT(1) AS total_votes" in sql_call
        assert "GROUP BY year, week_number" in sql_call
        assert "ORDER BY year, week_number ASC" in sql_call
        assert f"WHERE abs(1 - total_votes / avg) > {OUTLIER_THRESHOLD}" in sql_call


class TestOutliersIntegration(unittest.TestCase):

    def setUp(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")
    
    def tearDown(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")
    
    def test_create_outliers_view_creates_view_successfully(self):
        
        with get_connection() as conn:
            setup_schema_and_table(conn)
            
            conn.execute(f"""
                INSERT INTO {SCHEMA_NAME}.{MAIN_TABLE_NAME} 
                (id, user_id, post_id, vote_type_id, bounty_amount, creation_date)
                VALUES 
                ('1', 'user1', 'post1', 1, 0.0, '2022-01-01 00:00:00'),
                ('2', 'user2', 'post2', 2, 0.0, '2022-01-08 00:00:00'),
                ('3', 'user3', 'post3', 1, 0.0, '2022-01-15 00:00:00')
            """)
            
            create_outliers_view(conn)
            
            result = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{OUTLIER_WEEKS_VIEW_NAME}").fetchall()
            assert isinstance(result, list)
    
    def test_outliers_view_handles_empty_data(self):
        
        with get_connection() as conn:
            setup_schema_and_table(conn)
            create_outliers_view(conn)
            result = conn.execute(f"SELECT * FROM {SCHEMA_NAME}.{OUTLIER_WEEKS_VIEW_NAME}").fetchall()
            assert result == []


class TestOutlierCalculationIntegration(unittest.TestCase):

    def setUp(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")

    def tearDown(self):
        if os.path.exists("warehouse.db"):
            os.remove("warehouse.db")

    def test_outlier_calculation_is_correct_for_sample_data(self):
        ingest_result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.ingest",
                "tests/test-resources/samples-votes.jsonl",
            ],
            capture_output=True,
        )

        assert ingest_result.returncode == 0, f"Ingestion failed: {ingest_result.stderr.decode()}"

        outliers_result = subprocess.run(
            args=[
                "python",
                "-m",
                "equalexperts_dataeng_exercise.outliers"
            ],
            capture_output=True,
        )

        assert outliers_result.returncode == 0, f"Outliers analysis failed: {outliers_result.stderr.decode()}"

        sql = "SELECT * FROM blog_analysis.outlier_weeks;"
        con = duckdb.connect("warehouse.db", read_only=True)
        query_result = con.sql(sql)
        actual_results = query_result.fetchall()
        con.close()

        expected_results = [
            (2022, 0, 1), (2022, 1, 3), (2022, 2, 3), (2022, 5, 1), (2022, 6, 1), (2022, 8, 1)
        ]

        assert actual_results == expected_results, "Expected view 'outlier_weeks' to have correct output for sample data"


@pytest.fixture(autouse=True)
def delete_existing_db():
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")
    yield
    if os.path.exists("warehouse.db"):
        os.remove("warehouse.db")

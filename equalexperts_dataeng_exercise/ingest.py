import sys
import os
import duckdb
import json
from equalexperts_dataeng_exercise.db import get_connection, setup_schema_and_table, SCHEMA_NAME, MAIN_TABLE_NAME, \
    WAREHOUSE_PATH

ARGUMENTS_COUNT = 2
FILE_PATH_ARGUMENT_INDEX = 1
STAGE_TABLE_NAME = "votes_stage"


def validate_arguments(args: list[str]) -> None:
    if len(args) != ARGUMENTS_COUNT:
        raise ValueError("Usage: python ingest.py <file_path>")
    
def validate_file_path(file_path: str) -> None:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} does not exist")

def validate_file_has_required_columns(file_path: str) -> bool:
    required_columns = {"Id", "PostId", "VoteTypeId", "CreationDate"}
    with open(file_path, "r", encoding="utf-8") as data:
        first_line = data.readline()
        if first_line:
            columns = json.loads(first_line)
            return all(col in columns for col in required_columns)
    return False

def create_stage_table_from_file(file_path: str, conn: duckdb.DuckDBPyConnection) -> None:
    stage_table_query = f"""
        CREATE OR REPLACE TABLE {SCHEMA_NAME}.{STAGE_TABLE_NAME} AS
        WITH raw AS (
            SELECT * FROM
            read_json_auto(
                '{file_path}',
                columns={{
                    'Id': 'STRING',
                    'UserId': 'STRING',
                    'PostId': 'STRING',
                    'VoteTypeId': 'INTEGER',
                    'BountyAmount': 'DOUBLE',
                    'CreationDate': 'TIMESTAMP'
                }}
            )
        )
        SELECT id, user_id, post_id, vote_type_id, bounty_amount, creation_date FROM (
            SELECT CAST(Id AS STRING) AS id,
                    CAST(UserId AS STRING) AS user_id,
                    CAST(PostId AS STRING) AS post_id,
                    CAST(VoteTypeId AS INTEGER) AS vote_type_id,
                    CAST(BountyAmount AS DOUBLE) AS bounty_amount,
                    CAST(CreationDate AS TIMESTAMP) AS creation_date,
                    ROW_NUMBER() OVER (PARTITION BY id ORDER BY creation_date DESC) AS row_number
            FROM raw
        ) a WHERE a.row_number = 1;
    """

    conn.execute(stage_table_query)

def update_main_table_from_stage_table(conn: duckdb.DuckDBPyConnection) -> None:
    insert_query = f"""
        INSERT OR REPLACE INTO {SCHEMA_NAME}.{MAIN_TABLE_NAME} 
            (id, user_id, post_id, vote_type_id, bounty_amount, creation_date)
        SELECT
            id,
            user_id,
            post_id,
            vote_type_id,
            bounty_amount,
            creation_date
        FROM {SCHEMA_NAME}.{STAGE_TABLE_NAME}
    """
    conn.execute(insert_query)
    
def drop_stage_table(conn: duckdb.DuckDBPyConnection) -> None:
    drop_query = f"""
        DROP TABLE IF EXISTS {SCHEMA_NAME}.{STAGE_TABLE_NAME};
    """
    conn.execute(drop_query)

def ingest_data(file_path: str, conn: duckdb.DuckDBPyConnection) -> None:
    if not validate_file_has_required_columns(file_path):
        raise ValueError(f"File {file_path} does not have required columns for ingestion")

    create_stage_table_from_file(file_path, conn)
    update_main_table_from_stage_table(conn)
    # drop_stage_table(conn)

def start_ingestion(warehouse_path: str, file_path: str) -> None:
    with get_connection(warehouse_path) as conn:
        setup_schema_and_table(conn)
        ingest_data(file_path, conn)
        # test_data(sys.argv[FILE_PATH_ARGUMENT_INDEX], conn)



if __name__ == "__main__":
    validate_arguments(sys.argv)
    validate_file_path(sys.argv[FILE_PATH_ARGUMENT_INDEX])

    start_ingestion(WAREHOUSE_PATH, sys.argv[FILE_PATH_ARGUMENT_INDEX])

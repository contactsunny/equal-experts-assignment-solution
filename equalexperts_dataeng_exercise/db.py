import duckdb

WAREHOUSE_PATH = "warehouse.db"
SCHEMA_NAME = "blog_analysis"
MAIN_TABLE_NAME = "votes"
# Dead letter queue table
DLQ_TABLE_NAME = "votes_dlq"


def get_connection(warehouse_path: str):
    return duckdb.connect(warehouse_path)

def setup_schema_and_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{MAIN_TABLE_NAME} (
                id STRING NOT NULL PRIMARY KEY, 
                user_id STRING,
                post_id STRING NOT NULL, 
                vote_type_id INTEGER NOT NULL, 
                bounty_amount DOUBLE,
                creation_date TIMESTAMP NOT NULL
            );
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{DLQ_TABLE_NAME} (
                id STRING, 
                user_id STRING,
                post_id STRING, 
                vote_type_id INTEGER, 
                bounty_amount DOUBLE,
                creation_date TIMESTAMP
            );
            """)

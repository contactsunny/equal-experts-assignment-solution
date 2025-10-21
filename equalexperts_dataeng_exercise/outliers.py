import duckdb
from equalexperts_dataeng_exercise.db import get_connection, SCHEMA_NAME, MAIN_TABLE_NAME

OUTLIER_WEEKS_VIEW_NAME = "outlier_weeks"
OUTLIER_THRESHOLD = 0.2
WEEK_NUMBER_MODULO = 52


def create_outliers_view(conn: duckdb.Connection) -> None:
    sql = f"""
        CREATE OR REPLACE VIEW {SCHEMA_NAME}.{OUTLIER_WEEKS_VIEW_NAME} AS
        WITH weekly_total AS (
            SELECT 
                EXTRACT(YEAR FROM creation_date) AS year, 
                EXTRACT(WEEK FROM creation_date) % {WEEK_NUMBER_MODULO} AS week_number,
                COUNT(1) AS total_votes 
            FROM {SCHEMA_NAME}.{MAIN_TABLE_NAME} 
            GROUP BY year, week_number 
            ORDER BY year, week_number ASC
        ),
        average_votes AS (
            SELECT AVG(total_votes) AS avg FROM weekly_total
        ),
        outliers AS (
            SELECT year, week_number, total_votes
            FROM weekly_total, average_votes
            WHERE abs(1 - total_votes / avg) > {OUTLIER_THRESHOLD}
        )
        SELECT * FROM outliers ORDER BY year, week_number ASC;
    """
    conn.execute(sql)

def get_outlier_weeks(conn: duckdb.Connection) -> None:
    print(conn.sql(f"SELECT * FROM {SCHEMA_NAME}.{OUTLIER_WEEKS_VIEW_NAME}").fetchdf())


if __name__ == "__main__":
    with get_connection() as conn:
        create_outliers_view(conn)
        get_outlier_weeks(conn)

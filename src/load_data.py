import os
import psycopg2
import pandas as pd
import sqlalchemy
import io
import duckdb

from .logger import logger


def create_table_with_pandas(
    data_dir: str, filename: str, table_name: str, engine: sqlalchemy.engine.base.Engine
) -> None:
    file_path = os.path.join(data_dir, filename)
    df = pd.read_parquet(file_path)
    df.head(0).to_sql(table_name, engine, if_exists="fail", index=False, schema='public')


def load_parquet_to_postgres(
    data_dir: str,
    filename: str,
    table_name: str,
    conn: psycopg2.extensions.connection,
) -> None:
    cursor = conn.cursor()
    file_path = os.path.join(data_dir, filename)

    try:
        df = pd.read_parquet(file_path)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        copy_sql = f"COPY public.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','"
        cursor.copy_expert(copy_sql, csv_buffer)
        conn.commit()
        cursor.close()

    except Exception as e:
        conn.rollback()
        cursor.close()
        logger.error(f"Error loading Parquet: {e}")


def load_csv_to_postgres(
    data_dir: str, filename: str, table_name: str, conn: psycopg2.extensions.connection
) -> None:
    cursor = conn.cursor()
    file_path = os.path.join(data_dir, filename)

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            cursor.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f
            )
        cursor.close()

    except Exception as e:
        cursor.close()
        logger.error(f"Error loading CSV: {e}")


def load_parquet_to_duckdb(data_dir, filename, db_path="wmcsrp2.duckdb", schema="public"):
    table_name = filename.replace(".parquet", "")
    parquet_path = os.path.join(data_dir, filename)

    logger.info(f"Creating DuckDB table {table_name} from {parquet_path}")

    conn = duckdb.connect(db_path)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"""
        CREATE OR REPLACE TABLE {schema}.{table_name} AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    conn.close()
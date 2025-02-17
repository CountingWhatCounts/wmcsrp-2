import os
import psycopg2
import pandas as pd
import sqlalchemy


def create_table_with_pandas(
        data_dir: str,
        csv_file: str,
        table_name: str,
        engine: sqlalchemy.engine.base.Engine
    ) -> None:

    file_path = os.path.join(data_dir, csv_file)    
    df = pd.read_csv(file_path)
    df.head(0).to_sql(table_name, engine, if_exists="fail", index=False)


def load_csv_to_postgres(
        data_dir: str,
        csv_file: str,
        table_name: str,
        conn: psycopg2.extensions.connection
    ) -> None:

    cursor = conn.cursor()
    file_path = os.path.join(data_dir, csv_file)
    with open(file_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER DELIMITER ','", f)
    cursor.close()
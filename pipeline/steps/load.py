from src.logger import logger
from pipeline.config import PREPROCESSED_DATA_DIR, DATA_BACKEND
from pipeline.steps.preprocess import data_spec


def run():
    logger.info("======== LOADING DATA ========")

    if DATA_BACKEND == "postgres":
        from pipeline.config import POSTGRES_URL
        import psycopg2
        from sqlalchemy import create_engine
        from src.load_data import create_table_with_pandas, load_parquet_to_postgres

        engine = create_engine(POSTGRES_URL)

        for filename in data_spec:
            table_name = filename.replace(".parquet", "")
            try:
                create_table_with_pandas(
                    data_dir=PREPROCESSED_DATA_DIR,
                    filename=filename,
                    table_name=table_name,
                    engine=engine,
                )

                with psycopg2.connect(POSTGRES_URL) as conn:
                    logger.info(f"Loading data for {filename}")
                    load_parquet_to_postgres(
                        data_dir=PREPROCESSED_DATA_DIR,
                        filename=filename,
                        table_name=table_name,
                        conn=conn,
                    )
            except ValueError:
                logger.info(f"Table already present for {filename}")

    elif DATA_BACKEND == "duckdb":
        from src.load_data import load_parquet_to_duckdb

        for filename in data_spec:
            load_parquet_to_duckdb(
                data_dir=PREPROCESSED_DATA_DIR,
                filename=filename
            )
    else:
        logger.error(f"Unsupported DATA_BACKEND: {DATA_BACKEND}")
        raise ValueError(f"Unsupported backend: {DATA_BACKEND}")

    logger.info("======== DATA LOADED ========\n\n")
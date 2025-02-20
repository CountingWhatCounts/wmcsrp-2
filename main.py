import os
from dbt.cli.main import dbtRunner
import shutil
import psycopg2
from sqlalchemy import create_engine

from src.download_data import download_from_gcs
import src.preprocessing as preprocess
from src.logger import logger
from src.load_data import create_table_with_pandas, load_csv_to_postgres


if __name__ == "__main__":
    RAW_DATA_DIRECTORY = "raw_data"
    PREPROCESSED_DATA_DIRECTORY = "preprocessed_data"
    DB_CONN = f"postgresql://doadmin:{os.getenv('WMCSRP_DB_PASS')}@{os.getenv('POSTGRES_HOST')}:25060/wmcsrp2?sslmode=require"
    engine = create_engine(
        f"postgresql://doadmin:{os.getenv('WMCSRP_DB_PASS')}@{os.getenv('POSTGRES_HOST')}:25060/wmcsrp2?sslmode=require"
    )

    # Define the output file names and associated processing functions
    data_spec = {
        "raw__ace_project_grants.csv": preprocess.ace_project_grants,
        "raw__ace_npo_funding.csv": preprocess.ace_npo_funding,
        "raw__economic.csv": preprocess.economic_data,
        "raw__360giving.csv": preprocess.giving360_data,
        "raw__indices_of_deprivation.csv": preprocess.imd_data,
        "raw__cultural_infrastructure.csv": preprocess.cultural_infrastructure,
        "raw__wellbeing.csv": preprocess.wellbeing,
        "raw__census.csv": preprocess.census_data,
        "raw__rural_urban_classification.csv": preprocess.rural_urban_classification,
        "raw__ace_priority_places.csv": preprocess.ace_priority_places,
        "raw__postcode_mapping.csv": preprocess.postcode_mapping,
        "raw__msoa_mapping.csv": preprocess.msoa_mapping,
        "raw__msoa_population.csv": preprocess.msoa_population,
        "raw__ace_levelling_up_for_culture_places.csv": preprocess.ace_levelling_up_places,
    }

    # Download data from google cloud bucket
    logger.info("======== INITIALISING FILE DOWNLOAD ========")
    os.makedirs(RAW_DATA_DIRECTORY, exist_ok=True)
    download_from_gcs(
        bucket_name=os.getenv("WMCSRP_BUCKET"), downloaded_data_dir=RAW_DATA_DIRECTORY
    )
    logger.info("======== FILE DOWNLOAD COMPLETE =========\n\n")

    logger.info("======== PREPROCESSING DATA FILES ========")
    os.makedirs(PREPROCESSED_DATA_DIRECTORY, exist_ok=True)
    for file, processor in data_spec.items():
        if not os.path.exists(os.path.join(PREPROCESSED_DATA_DIRECTORY, file)):
            processor(
                downloaded_data_dir=RAW_DATA_DIRECTORY,
                seed_data_dir=PREPROCESSED_DATA_DIRECTORY,
                output_filename=file,
            )
        else:
            logger.info(f"{file} already present")
    logger.info("======== PREPROCESSING COMPLETED ========\n\n")

    # Load the processed data into the database
    # Use pandas to create the table, but use psycopg2 to load the data
    # Pandas easily creates a correctly specified table from a dataframe
    # posycopg2 uses copy instead of insert which is much faster
    logger.info("======== LOADING DATA ========")
    for csv_file, _ in data_spec.items():
        try:
            create_table_with_pandas(
                data_dir=PREPROCESSED_DATA_DIRECTORY,
                csv_file=csv_file,
                table_name=csv_file.split(".")[0],
                engine=engine,
            )

            with psycopg2.connect(DB_CONN) as conn:
                logger.info(f"Loading data for {csv_file}")
                load_csv_to_postgres(
                    data_dir=PREPROCESSED_DATA_DIRECTORY,
                    csv_file=csv_file,
                    table_name=csv_file.split(".")[0],
                    conn=conn,
                )

        except ValueError:
            logger.info(f"Table already present for {csv_file}")

    logger.info("======== DATA LOADED ========\n\n")

    logger.info("======== RUNNING DBT ========")
    dbt = dbtRunner()
    dbt.invoke(["run"])
    logger.info("======== RUN COMPLETE ========\n\n")

    logger.info("======== CLEANING DATA ========")
    try:
        shutil.rmtree(RAW_DATA_DIRECTORY)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

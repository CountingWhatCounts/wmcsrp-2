import os
from dbt.cli.main import dbtRunner
from google.cloud import storage
import shutil
import psycopg2
from sqlalchemy import create_engine

from src.download_data import download_from_gcs
import src.preprocessing as preprocess
from src.logger import logger
from src.load_data import create_table_with_pandas, load_parquet_to_postgres


if __name__ == "__main__":
    RAW_DATA_DIRECTORY = "raw_data"
    PREPROCESSED_DATA_DIRECTORY = "preprocessed_data"
    DB_CONN = f"postgresql://doadmin:{os.getenv('WMCSRP_DB_PASS')}@{os.getenv('POSTGRES_HOST')}:25060/wmcsrp2?sslmode=require"
    engine = create_engine(
        f"postgresql://doadmin:{os.getenv('WMCSRP_DB_PASS')}@{os.getenv('POSTGRES_HOST')}:25060/wmcsrp2?sslmode=require"
    )

    # Define the output file names and associated processing functions
    data_spec = {
        "raw__ace_project_grants.parquet": preprocess.ace_project_grants,
        "raw__ace_npo_funding.parquet": preprocess.ace_npo_funding,
        "raw__economic.parquet": preprocess.economic_data,
        "raw__360giving.parquet": preprocess.giving360_data,
        "raw__indices_of_deprivation.parquet": preprocess.imd_data,
        "raw__cultural_infrastructure.parquet": preprocess.cultural_infrastructure,
        "raw__wellbeing.parquet": preprocess.wellbeing,
        "raw__census.parquet": preprocess.census_data,
        "raw__rural_urban_classification.parquet": preprocess.rural_urban_classification,
        "raw__ace_priority_places.parquet": preprocess.ace_priority_places,
        "raw__postcode_mapping.parquet": preprocess.postcode_mapping,
        "raw__msoa_mapping.parquet": preprocess.msoa_mapping,
        "raw__msoa_population.parquet": preprocess.msoa_population,
        "raw__ace_levelling_up_for_culture_places.parquet": preprocess.ace_levelling_up_places,
        "raw__participation_survey_data.parquet": preprocess.participation_survey_data,
        "raw__participation_survey_variable_dictionary.parquet": preprocess.participation_survey_variable_dictionary,
        "raw__participation_survey_values_dictionary.parquet": preprocess.participation_survey_values_dictionary,
        "raw__community_life_survey.parquet": preprocess.community_life_survey,
        "raw__community_life_survey_benchmarks.parquet": preprocess.community_life_survey_benchmarks,
        "raw__modelled_participation_statistics.parquet": preprocess.modelled_participation_statistics,
        "raw__dcms_participation_statistics.parquet": preprocess.participation_survey_dcms_data_tables,
        "raw__residents_survey_local_authority_results.parquet": preprocess.residents_survey_local_authority_results,
        "raw__region_populations.parquet": preprocess.region_populations,
    }

    to_run = ["download", "preprocess", "load"]  # , "dbt"]  # , "clean"]

    # Download data from google cloud bucket
    if "download" in to_run:
        logger.info("======== INITIALISING FILE DOWNLOAD ========")
        os.makedirs(RAW_DATA_DIRECTORY, exist_ok=True)
        client = storage.Client.create_anonymous_client()
        bucket = client.bucket(os.getenv("WMCSRP_BUCKET"))
        blobs = bucket.list_blobs()
        for blob in blobs:
            download_from_gcs(blob=blob, downloaded_data_dir=RAW_DATA_DIRECTORY)
        logger.info("======== FILE DOWNLOAD COMPLETE =========\n\n")

        if "preprocess" in to_run:
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
    if "load" in to_run:
        logger.info("======== LOADING DATA ========")
        for filename, _ in data_spec.items():
            try:
                create_table_with_pandas(
                    data_dir=PREPROCESSED_DATA_DIRECTORY,
                    filename=filename,
                    table_name=filename.split(".")[0],
                    engine=engine,
                )

                with psycopg2.connect(DB_CONN) as conn:
                    logger.info(f"Loading data for {filename}")
                    load_parquet_to_postgres(
                        data_dir=PREPROCESSED_DATA_DIRECTORY,
                        filename=filename,
                        table_name=filename.split(".")[0],
                        conn=conn,
                    )
            except ValueError:
                logger.info(f"Table already present for {filename}")
        logger.info("======== DATA LOADED ========\n\n")

    if "dbt" in to_run:
        logger.info("======== RUNNING DBT ========")
        dbt = dbtRunner()
        dbt.invoke(["run"])
        logger.info("======== RUN COMPLETE ========\n\n")

    if "clean" in to_run:
        logger.info("======== CLEANING DATA ========")
        try:
            shutil.rmtree(RAW_DATA_DIRECTORY)
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))
        try:
            shutil.rmtree(PREPROCESSED_DATA_DIRECTORY)
        except OSError as e:
            logger.error("Error: %s - %s." % (e.filename, e.strerror))

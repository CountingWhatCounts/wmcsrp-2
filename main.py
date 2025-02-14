import os
from dbt.cli.main import dbtRunner
import shutil

from src.download_data import download_from_gcs
import src.preprocessing as preprocess
from src.logger import logger



if __name__ == '__main__':

    RAW_DATA_DIRECTORY='raw_data'
    SEED_DATA_DIRECTORY='seeds'
    dbt = dbtRunner()
    


    # Define the output file names and associated processing functions
    processing_spec = {
        'seed_ace_project_grants.csv': preprocess.ace_project_grants,
        'seed_ace_npo_funding.csv': preprocess.ace_npo_funding,
        'seed_economic.csv': preprocess.economic_data,
        'seed_grant360.csv': preprocess.grant360_data,
        'seed_indices_of_deprivation.csv': preprocess.imd_data,
        'seed_cultural_infrastructure.csv': preprocess.cultural_infrastructure,
        'seed_wellbeing.csv': preprocess.wellbeing,
        'seed_census.csv': preprocess.census_data,
        'seed_rural_urban_classification.csv': preprocess.rural_urban_classification,
        'seed_ace_priority_places.csv': preprocess.ace_priority_places,
        'seed_postcode_mapping.csv': preprocess.postcode_mapping,
        'seed_msoa_mapping.csv': preprocess.msoa_mapping,
        'seed_msoa_population.csv': preprocess.msoa_population
    }



    # Download data from google cloud bucket
    logger.info("\n======== INITIALISING FILE DOWNLOAD ========\n")
    os.makedirs(RAW_DATA_DIRECTORY, exist_ok=True)
    download_from_gcs(bucket_name=os.getenv('WMCSRP_BUCKET'),
                      downloaded_data_dir=RAW_DATA_DIRECTORY)
    logger.info("\n======== FILE DOWNLOAD COMPLETE =========\n")



    # Preprocess each of the downloaded data files
    logger.info("\n======== PREPROCESSING DATA FILES ========\n")
    os.makedirs(SEED_DATA_DIRECTORY, exist_ok=True)
    for file, processor in processing_spec.items():
        if not os.path.exists(os.path.join(SEED_DATA_DIRECTORY, file)):
            processor(downloaded_data_dir=RAW_DATA_DIRECTORY,
                    seed_data_dir=SEED_DATA_DIRECTORY,
                    output_filename=file)
        else:
            logger.info(f"{file} already present")
    logger.info("\n======== PREPROCESSING COMPLETED ========\n")



    dbt.invoke('seed')
    dbt.invoke('build')
    

    try:
        shutil.rmtree(RAW_DATA_DIRECTORY)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))

    try:
        shutil.rmtree(SEED_DATA_DIRECTORY)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
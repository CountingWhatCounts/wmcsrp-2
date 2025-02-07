import os
from dbt.cli.main import dbtRunner, dbtRunnerResult

from src.download_data import download_from_gcs
from src.helper_functions import get_config
from src.logger import logger
import src.preprocessing as preprocess



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



if __name__ == '__main__':
    
    config = get_config('config.yml')

    # Download data from google cloud bucket
    logger.info("")
    logger.info("======== INITIALISING FILE DOWNLOAD ========")
    logger.info("")
    download_from_gcs(bucket_name=config.get('gcp_bucket'),
                      downloaded_data_dir=config.get('downloaded_data_dir'))
    logger.info("")
    logger.info("======== FILE DOWNLOAD COMPLETE =========")
    logger.info("")

    # Create the directory to store the preprocessed data
    os.makedirs(config.get('preprocessed_data_dir'), exist_ok=True)


    # Preprocess each of the downloaded data files
    logger.info("")
    logger.info("======== PREPROCESSING DATA FILES ========")
    logger.info("")
    for file, processing_function in processing_spec.items():
        if not os.path.exists(os.path.join(config.get('preprocessed_data_dir'), file)):
            processing_function(
                config.get('downloaded_data_dir'),
                config.get('preprocessed_data_dir'),
                file)
        else:
            logger.info(f"{file} already present")
    logger.info("")
    logger.info("======== PREPROCESSING COMPLETED ========")
    logger.info("")


    # Run the dbt data modelling
    logger.info("")
    logger.info("======== RUNNING DBT MODELLING ========")
    logger.info("")
    dbt = dbtRunner()
    cli_args = ["build"]
    dbt.invoke(cli_args)
    logger.info("")
    logger.info("======== DBT MODELLING COMPLETED ========")
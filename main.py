from dotenv import load_dotenv
import os

from src.download_data import download_from_gcs
from src.helper_functions import get_config
from src.logger import logger
import src.preprocessing as preprocess


if __name__ == '__main__':
    
    load_dotenv(dotenv_path='.env', override=True)
    config = get_config('config.yml')

    download_from_gcs(
        bucket_name=config.get('gcp_bucket'),
        downloaded_data_dir=config.get('downloaded_data_dir')
    )

    processing_spec = {
        'ace_project_grants.csv': preprocess.ace_project_grants,
        'ace_npo_funding.csv': preprocess.ace_npo_funding,
        'economic.csv': preprocess.economic_data,
        'grant360.csv': preprocess.grant360_data,
        'imd.csv': preprocess.imd_data,
        'cultural_infrastructure.csv': preprocess.cultural_infrastructure,
        'wellbeing.csv': preprocess.wellbeing,
        'census.csv': preprocess.census_data,
        'yougov_survey.csv': preprocess.yougov,
    }

    for file, processing_function in processing_spec.items():
        if not os.path.exists(os.path.join(config.get('preprocessed_data_dir'), file)):
            processing_function(
                config.get('downloaded_data_dir'),
                config.get('preprocessed_data_dir'),
                file)
        else:
            logger.info(f"{file} already present")
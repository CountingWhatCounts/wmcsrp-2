from dotenv import load_dotenv

from src.download_data import download_from_gcs
from src.helper_functions import get_config
import src.preprocessing as preprocess


if __name__ == '__main__':
    
    load_dotenv(dotenv_path='.env', override=True)
    config = get_config('config.yml')


    download_from_gcs(
        bucket_name=config.get('gcp_bucket'),
        downloaded_data_dir=config.get('downloaded_data_dir')
    )


    preprocess.ace_project_grants(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    preprocess.ace_npo_funding(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    preprocess.economic_data(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    preprocess.grant360_data(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    # preprocess.imd_data(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    # preprocess.cultural_infrastructure(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    # preprocess.wellbeing(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    # preprocess.yougov(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
    # preprocess.census_data(config.get('downloaded_data_dir'), config.get('preprocessed_data_dir'))
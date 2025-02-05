from dotenv import load_dotenv

from src.download_data import download_from_gcs
from src.helper_functions import get_config



if __name__ == '__main__':
    
    load_dotenv(dotenv_path='.env', override=True)
    config = get_config('config.yml')


    download_from_gcs(
        bucket_name=config.gcp_bucket,
        downloaded_data_dir=config.downloaded_data_dir
    )

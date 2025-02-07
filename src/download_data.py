from google.cloud import storage
import os
from .logger import logger
from .helper_functions import get_config



def download_from_gcs(
        bucket_name: str,
        downloaded_data_dir: str
    ) -> None:
    
    """Downloads all files from a GCS bucket to a local directory."""
    
    os.makedirs(downloaded_data_dir, exist_ok=True)

    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    for blob in blobs:
        if os.path.exists(os.path.join(downloaded_data_dir, blob.name)):
            logger.info(f"File already exists: {blob.name}")
        else:
            local_path = os.path.join(downloaded_data_dir, blob.name)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)
            logger.info(f"Downloaded: {blob.name} → {local_path}")



if __name__ == '__main__':
    config = get_config('config.yml')
    download_from_gcs(bucket_name=config.get('gcp_bucket'), downloaded_data_dir='data/downloaded')
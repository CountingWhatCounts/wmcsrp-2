from google.cloud import storage
import os
from src.download_data import download_from_gcs
from src.logger import logger
from pipeline.config import RAW_DATA_DIR, BUCKET_NAME

def run():
    logger.info("======== INITIALISING FILE DOWNLOAD ========")
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs()
    for blob in blobs:
        download_from_gcs(blob=blob, downloaded_data_dir=RAW_DATA_DIR)
    logger.info("======== FILE DOWNLOAD COMPLETE =========\n\n")
import os
from google.cloud.storage.blob import Blob
from .logger import logger


def download_from_gcs(blob: Blob, downloaded_data_dir: str) -> None:
    """Downloads all files from a GCS bucket to a local directory."""

    if blob.name.endswith("/"):
        logger.info(f"Skipping directory blob: {blob.name}")
        return

    if os.path.exists(os.path.join(downloaded_data_dir, blob.name)):
        logger.info(f"File already exists: {blob.name}")
    else:
        local_path = os.path.join(downloaded_data_dir, blob.name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded: {blob.name} → {local_path}")

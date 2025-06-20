import shutil
from src.logger import logger
from pipeline.config import RAW_DATA_DIR, PREPROCESSED_DATA_DIR

def run():
    logger.info("======== CLEANING DATA ========")
    for path in [RAW_DATA_DIR, PREPROCESSED_DATA_DIR]:
        try:
            shutil.rmtree(path)
            logger.info(f"Deleted {path}")
        except OSError as e:
            logger.error(f"Error deleting {path}: {e}")
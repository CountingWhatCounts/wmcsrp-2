import logging


def setup_logger():
    logger = logging.getLogger("shared_logger")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

logger = setup_logger()
# etl_logger.py
import logging
from logging.handlers import RotatingFileHandler
import os

# Ensure logs directory exists
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name="ETL", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    log_path = os.path.join(LOG_DIR, "etl.log")
    handler = RotatingFileHandler(log_path, maxBytes=2*1024*1024, backupCount=5)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)

    if not logger.handlers:  # Prevent duplicate handlers
        logger.addHandler(handler)
        logger.addHandler(logging.StreamHandler())  # Still see logs in console

    return logger

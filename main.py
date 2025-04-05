import os
import sys
import json
from loguru import logger
from utils.datastats_utils import DataStats
from utils.config_loader import Config
from utils.gcp_utils import GoogleUtils
from google.cloud import logging as gcloud_logging

# Initialize Google Cloud Logging
client = gcloud_logging.Client()
client.setup_logging()


if __name__ == "__main__":
    pass

    # ------------------------------------------------------------------------------------------------------------------
    # Set config & env vars
    # ------------------------------------------------------------------------------------------------------------------
    
    try:
        config = Config.load()
    except EnvironmentError as e:
        logger.error(f'Error while generating config: {e}')
        sys.exit(1)
    
    # ------------------------------------------------------------------------------------------------------------------
    # Scrape jobs and insert data in Postgres
    # ------------------------------------------------------------------------------------------------------------------    

    datastats = DataStats(config=config)
    datastats.process_scraped_files()
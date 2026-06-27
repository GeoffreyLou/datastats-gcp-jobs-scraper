import sys
from loguru import logger
from config import config
from utils import DataStats
from google.cloud import logging as gcloud_logging

# Initialize Google Cloud Logging
client = gcloud_logging.Client()
client.setup_logging()

if __name__ == "__main__":

    # ------------------------------------------------------------------------------------------------------------------
    # Scrape jobs and insert data 
    # ------------------------------------------------------------------------------------------------------------------    

    try:
        datastats = DataStats(
            project_id=config.PROJECT_ID,
            bq_dataset=config.BQ_DATASET,
            urls_bucket_name=config.DATASTATS_BUCKET_URLS,
            archive_bucket_name=config.DATASTATS_BUCKET_ARCHIVE,
            jobs_information_table_name=config.JOBS_INFORMATION_TABLE_NAME,
            jobs_information_config=config.JOBS_INFORMATION_CONFIG,
            jobs_description_table_name=config.JOBS_DESCRIPTION_TABLE_NAME,
            jobs_description_config=config.JOBS_DESCRIPTION_CONFIG,
            scrap_errors_table_name=config.ERRORS_TABLE_NAME,
            scrap_errors_config=config.ERRORS_CONFIG
        )
        datastats.start_workflow()
    except Exception as e:
        logger.error(f'Error while scraping jobs: {e}')
        sys.exit(1)
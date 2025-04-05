import json
from loguru import logger
from utils.config_loader import Config
from utils.gcp_utils import GoogleUtils
from utils.pg_utils import PostgresUtils
from utils.jobs_scraper import JobsScraper

class DataStats:
    def __init__(
        self,
        config: Config
    ):
        self.urls_bucket_name=config.DATASTATS_BUCKET_URLS
        self.db_host=config.DB_HOST,
        self.db_user=config.DB_USER,
        self.db_password=config.DB_USER_PASSWORD,
        self.db_name=config.DB_NAME,
        self.db_port=config.DB_PORT,
        self.db_root_cert=config.DB_ROOT_CERT,
        self.db_cert=config.DB_CERT,
        self.db_key=config.DB_KEY    

    def __generate_jobs_to_scrap(self, file: str) -> list(dict[str, str, str]):
        """
        Generate a list of jobs to scrap from a file.
        The file param is a JSON string that contains the following structure:
        {
            "date": "2023-10-01",
            "job": {
                "Data Engineer": [
                    "https://example.com/job1",
                    "https://example.com/job2"
                ]
            }
        }
        
        The function will return a list of dictionaries with the following structure:
        [
            {
                "date": "2023-10-01",
                "job": "Data Engineer",
                "url": "https://example.com/job1"
            },
            {
                "date": "2023-10-01",
                "job": "Data Engineer",
                "url": "https://example.com/job2"
            }
        ]
        
        Parameters
        ----------
        file : str
            A JSON string containing the job data.
        
        Returns
        -------
        list(dict[str, str, str])
            A list of dictionaries containing the date, job title, and URL for each job.
        """
        
        try:
            file = json.loads(file)
            date = file["date"]
            job = list(file["job"].keys())[0]
            urls = file['job'][job]
            jobs_to_scrap = []

            for url in urls:
                jobs_to_scrap.append(
                    {
                        'date': date,
                        'job': list(file["job"].keys())[0],
                        'url': url
                    }
                )
            
            return jobs_to_scrap
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return []
        except Exception as e:
            print(f"An error occurred: {e}")
            return []
        
    
    def process_scraped_files(self):
        
        # List all files in the bucket.
        blobs_list = GoogleUtils.list_blobs(
            bucket_name=self.urls_bucket_name
        )

        # Generate the connection to Postgres and create tables
        pg = PostgresUtils()
        connection = pg.connect_with_ssl(
            db_host=self.db_host,
            db_user=self.db_user,
            db_password=self.db_password,
            db_name=self.db_name,
            db_port=self.db_port,
            db_root_cert=self.db_root_cert,
            db_cert=self.db_cert,
            db_key=self.db_key
        )
        pg.create_tables(connection)
        
        for blob in blobs_list:
            file_to_process = GoogleUtils.download_blob_as_string(
                bucket_name=self.urls_bucket_name,
                source_blob_name=blob,
            )

            # Normalize the file to have one dict per job
            jobs_to_scrap = self.__generate_jobs_to_scrap(file_to_process)

            # Initialize the JobsScraper with the list of jobs to scrap
            jobs_scraper = JobsScraper(jobs_to_scrap=jobs_to_scrap)
            
            # For each job , scrap job informations
            jobs_to_insert = jobs_scraper.scrape_jobs()
            
            # If there is an error, skip the job and upload url in Database to check it later
            for job in jobs_to_insert:
                if 'ValueNotFound' in job.values():
                    logger.warning(f"Job with value not allowed: {job.get('url')}")
                    continue
                else:
                    # If not, insert job data in the database
                    try:
                        job_id = pg.insert_job_data(connection, job)
                        logger.info(f"Inserted job ID: {job_id}")
                    except Exception as e:
                        logger.error(f"Failed to insert job: {e}")

            logger.info('Closing Postgres connection...')
            pg.close_connection(connection)
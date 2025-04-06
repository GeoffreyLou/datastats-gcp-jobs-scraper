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
        self.db_host=config.DB_HOST
        self.db_user=config.DB_USER
        self.db_password=config.DB_USER_PASSWORD
        self.db_name=config.DB_NAME
        self.db_port=config.DB_PORT
        self.db_root_cert=config.DB_ROOT_CERT
        self.db_cert=config.DB_CERT
        self.db_key=config.DB_KEY
        self.jobs_information_table_name = 'jobs_information'
        self.jobs_information_schema = {
            'id': 'SERIAL PRIMARY KEY',
            'id_deduplication': 'VARCHAR(255) UNIQUE',
            'scrap_date': 'DATE',
            'job_scraped': 'VARCHAR(255)',
            'job_name': 'VARCHAR(255)',
            'company_name': 'VARCHAR(255)',
            'location': 'VARCHAR(255)',
            'level': 'VARCHAR(255)',
            'type': 'VARCHAR(255)',
            'category': 'TEXT',
            'sector': 'TEXT',
        }
        self.jobs_description_table_name = 'jobs_description'
        self.jobs_description_schema = {
            'id': 'SERIAL PRIMARY KEY',
            'id_job_information': 'INTEGER UNIQUE REFERENCES jobs_information(id)',
            'description': 'TEXT',
        }
        self.scrap_errors_table_name = 'scrap_errors'
        self.scrap_errors_schema = {    
            'id': 'SERIAL PRIMARY KEY',
            'error_message': 'TEXT',
            'url': 'TEXT',
        }

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
        
    def __create_tables(self) -> None:
        """
        Create the tables in the database if they do not exist.
        
        Returns
        -------
        None
        """
        
        logger.info('Setting connection to pgsql...')
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
        
        logger.info('Create tables if they do not exist...')
        pg.create_table_if_not_exists(
            connection=connection,
            table_name=self.jobs_information_table_name,
            table_schema=self.jobs_information_schema
        )
        
        pg.create_table_if_not_exists(
            connection=connection,
            table_name=self.jobs_description_table_name,
            table_schema=self.jobs_description_schema
        )
        
        pg.create_table_if_not_exists(  
            connection=connection,
            table_name=self.scrap_errors_table_name,
            table_schema=self.scrap_errors_schema
        )
        
        pg.close_connection(connection)
        
    def __insert_jobs_data(self, data_list: list[dict]) -> None:
        """
        Insert job data into the database.
        
        Parameters
        ----------
        data_list : list(dict)
            A list of dictionaries containing the job data to insert.
        
        Returns
        -------
        None
        """
        
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
        
        for job in data_list: 
            # If there is an error, skip the job and upload url in Database to check it later
            if 'ValueNotFound' in job.values():
                try:
                    logger.warning(f"Job with value not allowed: {job.get('url')}")
                    error_keys = ', '.join([key for key, value in job.items() if value == 'ValueNotFound'])
                    
                    error_data = {
                        'error_message': error_keys,
                        'url': job['url']
                    }
                
                    pg.insert_data(
                        connection=connection, 
                        table_name=self.scrap_errors_table_name, 
                        data=error_data
                    )
                except Exception as e:
                    logger.error(f"Failed to insert error record: {e}")
                    continue
            # If not, the job is valid, insert it into the database
            else:
                try:
                    job_info_data = {
                        'id_deduplication': job['id_deduplication'],
                        'scrap_date': job['scrap_date'],
                        'job_scraped': job['job_scraped'],
                        'job_name': job['job_name'],
                        'company_name': job['company_name'],
                        'location': job['location'],
                        'level': job['level'],
                        'type': job['type'],
                        'category': job['category'],
                        'sector': job['sector']
                    }
                    
                    # The job_info_id is used to assure join between the two tables
                    job_info_id = pg.insert_data(
                        connection=connection,
                        table_name=self.jobs_information_table_name,
                        data=job_info_data
                    )
                    
                    job_description_data = {
                        'id_job_information': job_info_id,
                        'description': job['description']
                    }
                    
                    pg.insert_data(
                        connection=connection,
                        table_name=self.jobs_description_table_name,
                        data=job_description_data
                    )
                        
                    logger.info(f"Job successfully inserted with ID: {job_info_id}")
                except Exception as e:
                    logger.error(f"Failed to insert job data: {e}")
                    continue
     
        pg.close_connection(connection)
    
    def __scrap_urls(self):
        
        data_to_insert = []
        
        # List all files in the bucket and scrap data
        blobs_list = GoogleUtils.list_blobs(
            bucket_name=self.urls_bucket_name
        )

        for blob in blobs_list:
            file_to_process = GoogleUtils.download_blob_as_string(
                bucket_name=self.urls_bucket_name,
                source_blob_name=blob
            )

            # Normalize the file to have one dict per job
            jobs_to_scrap = self.__generate_jobs_to_scrap(file_to_process)

            # Initialize the JobsScraper with the list of jobs to scrap
            jobs_scraper = JobsScraper(jobs_to_scrap=jobs_to_scrap)
            
            # For each job , scrap job informations and add it to the list of jobs to insert
            jobs_to_insert = jobs_scraper.scrape_jobs()
            data_to_insert.extend(jobs_to_insert)

        return data_to_insert
    
    def start_workflow(self):
        
        # Create tables in the database
        self.__create_tables()
        
        # Scrap URLs and insert data in the database
        jobs_to_insert = self.__scrap_urls()
        
        self.__insert_jobs_data(data_list=jobs_to_insert)
        logger.success("DataStats workflow completed successfully.")
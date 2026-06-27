import json
import uuid
from datetime import datetime, timezone
from loguru import logger
from utils.gcp_utils import GoogleUtils
from utils.bigquery_utils import BigQueryTableManager
from utils.jobs_scraper import JobsScraper

class DataStats:
    def __init__(
        self,
        project_id: str,
        bq_dataset: str,
        urls_bucket_name: str,
        archive_bucket_name: str,
        jobs_information_table_name: str,
        jobs_information_config: dict,
        jobs_description_table_name: str,
        jobs_description_config: dict,
        scrap_errors_table_name: str,
        scrap_errors_config: dict
    ):
        self.urls_bucket_name = urls_bucket_name
        self.archive_bucket_name = archive_bucket_name
        self.jobs_information_table_name = jobs_information_table_name
        self.jobs_information_config = jobs_information_config
        self.jobs_description_table_name = jobs_description_table_name
        self.jobs_description_config = jobs_description_config
        self.scrap_errors_table_name = scrap_errors_table_name
        self.scrap_errors_config = scrap_errors_config
        self.bq = BigQueryTableManager(project_id=project_id, dataset_id=bq_dataset)

    def __generate_jobs_to_scrap(self, file: str) -> list[dict[str, str, str]]:
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
                        'job': job,
                        'url': url
                    }
                )

            return jobs_to_scrap
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return []

    def __create_tables(self) -> None:
        """
        Create the BigQuery dataset and tables if they do not exist.

        Returns
        -------
        None
        """

        logger.info('Checking BigQuery dataset and tables...')
        self.bq.create_dataset_if_not_exists()

        self.bq.create_table_from_schema(
            table_name=self.jobs_information_table_name,
            schema=self.jobs_information_config['schema'],
            description=self.jobs_information_config.get('description'),
            partition_field=self.jobs_information_config.get('partition_field'),
            clustering_fields=self.jobs_information_config.get('clustering_fields')
        )

        self.bq.create_table_from_schema(
            table_name=self.jobs_description_table_name,
            schema=self.jobs_description_config['schema'],
            description=self.jobs_description_config.get('description'),
            partition_field=self.jobs_description_config.get('partition_field'),
            clustering_fields=self.jobs_description_config.get('clustering_fields')
        )

        self.bq.create_table_from_schema(
            table_name=self.scrap_errors_table_name,
            schema=self.scrap_errors_config['schema'],
            description=self.scrap_errors_config.get('description'),
            partition_field=self.scrap_errors_config.get('partition_field'),
            clustering_fields=self.scrap_errors_config.get('clustering_fields')
        )

    def __insert_jobs_data(self, data_list: list[dict]) -> None:
        """
        Insert job data into BigQuery.

        Parameters
        ----------
        data_list : list(dict)
            A list of dictionaries containing the job data to insert.

        Returns
        -------
        None
        """

        for job in data_list:
            now = datetime.now(timezone.utc).isoformat()

            # If there is an error, skip the job and log the error for later review
            if 'ValueNotFound' in job.values():
                try:
                    logger.warning(f"Job with value not allowed: {job.get('url')}")
                    error_keys = ', '.join([key for key, value in job.items() if value == 'ValueNotFound'])

                    self.bq.insert_rows(
                        table_name=self.scrap_errors_table_name,
                        rows=[{
                            'id': str(uuid.uuid4()),
                            'error_message': error_keys,
                            'url': job['url'],
                            'created_at': now
                        }]
                    )
                except Exception as e:
                    logger.error(f"Failed to insert error record: {e}")
                    continue
            # If not, the job is valid, insert it into BigQuery
            else:
                try:
                    job_info_id = str(uuid.uuid4())

                    self.bq.insert_rows(
                        table_name=self.jobs_information_table_name,
                        rows=[{
                            'id': job_info_id,
                            'id_deduplication': job['id_deduplication'],
                            'scrap_date': job['scrap_date'],
                            'job_scraped': job['job_scraped'],
                            'job_name': job['job_name'],
                            'company_name': job['company_name'],
                            'location': job['location'],
                            'level': job['level'],
                            'type': job['type'],
                            'category': job['category'],
                            'sector': job['sector'],
                            'created_at': now,
                            'updated_at': now
                        }]
                    )

                    self.bq.insert_rows(
                        table_name=self.jobs_description_table_name,
                        rows=[{
                            'id_job_information': job_info_id,
                            'description': job['description'],
                            'created_at': now,
                            'updated_at': now
                        }]
                    )

                    logger.info(f"Job successfully inserted with ID: {job_info_id}")
                except Exception as e:
                    logger.error(f"Failed to insert job data: {e}")
                    continue

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

            # Move the processed blob to another bucket
            GoogleUtils.move_blob(
                source_bucket_name=self.urls_bucket_name,
                source_blob_name=blob,
                destination_bucket_name=self.archive_bucket_name
            )

        return data_to_insert

    def start_workflow(self):

        # Create the BigQuery dataset and tables
        self.__create_tables()

        # Scrap URLs and insert data in BigQuery
        jobs_to_insert = self.__scrap_urls()

        self.__insert_jobs_data(data_list=jobs_to_insert)
        logger.success("DataStats workflow completed successfully.")

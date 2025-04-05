import io
from loguru import logger
from google.cloud import storage

class GoogleUtils:
    def __init__(self) -> None:
        """
        Utilities for operations in Google Cloud Platform
        """
        pass

    @staticmethod
    def upload_file(
        bucket_name: str, 
        source_file_path: str, 
        destination_blob_name: str, 
        folder_path: str = ""
    ) -> None:
        """
        Upload a file to a GCP bucket. 

        Parameters
        ----------
        bucket_name: str
            Name of the bucket
        source_file_path: str
            Local path of the file to upload
        destination_blob_name: str
            Name of the file in the bucket
        folder_path: str
            Optional, the path of the blob in the bucket
        
        Returns
        -------
        None
        """
        try:
            blob_path = folder_path + destination_blob_name
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(source_file_path)
            logger.success(f"File {source_file_path} successfully uploaded as {blob_path} in bucket {bucket_name}.")
        except Exception as e:
            logger.error(f"Error when uploading file: {e}")
    
    @staticmethod
    def file_exists(
        bucket_name: str, 
        blob_name: str, 
        folder_path: str = ""
    ) -> bool:
        """
        Check if a file exists in a GCP bucket.

        Parameters
        ----------
        bucket_name: str
            Name of the bucket
        blob_name: str
            Name of the file in the bucket
        folder_path: str
            Optional, the path of the blob in the bucket

        Returns
        -------
        bool
            True if the file exists else False
        """
        try:
            blob_path = folder_path + blob_name if folder_path else blob_name
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            logger.info(f"Blob {blob_path} {'exists' if blob.exists() else 'does not exist'} in bucket {bucket_name}.")
            return True if blob.exists() else False
        except Exception as e:
            logger.error(f"Error occurred while checking for file existence: {e}")
        
    @staticmethod
    def upload_non_physical_file(
        bucket_name: str,
        data: str,
        destination_blob_name: str,
        content_type: str,
        folder_path: str = ""
    ) -> None:
        """
        Upload a non-physical file (e.g., JSON, CSV, TXT) to a GCP bucket.

        Parameters
        ----------
        bucket_name: str 
            Name of the bucket
        data: str
            Data to upload as a string
        destination_blob_name: str
            Name of the file in the bucket
        content_type: str
            Content type of the file (e.g., 'application/json', 'text/csv')
        folder_path: str
            Optional, the path of the blob in the bucket

        Returns
        -------
        None
        """
        try:
            blob_path = folder_path + destination_blob_name if folder_path else destination_blob_name
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.upload_from_string(data, content_type=content_type)
            logger.success(f"File successfully uploaded as {blob_path} in bucket {bucket_name}.")
        except Exception as e:
            logger.error(f"Error when uploading file: {e}")
            
    @staticmethod
    def download_blob(
        bucket_name: str, 
        source_blob_name: str, 
        destination_file_name: str,
        folder_path: str = ""
    ) -> None:
        """Downloads a blob from a bucket.
        
        Parameters
        ----------
        bucket_name: str
            Name of the bucket
        source_blob_name: str
            Name of the blob to download
        destination_file_name: str
            Name of the file to download from the bucket 
        folder_path: str
            Optional, the path of the blob in the bucket

        Returns
        -------
        None
        """
        try:
            blob_path = folder_path + source_blob_name if folder_path else source_blob_name
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            blob.download_to_filename(destination_file_name)
            logger.success(f"Downloaded storage object {source_blob_name} from bucket {bucket_name} to local file {destination_file_name}.")
        except Exception as e:
            logger.error(f"Error when downloading file: {e}")
            
    @staticmethod
    def download_blob_as_string(
        bucket_name: str, 
        source_blob_name: str, 
        folder_path: str = ""
    ) -> str:
        """
        Downloads a blob from a bucket and returns its content as a string.
        
        Parameters
        ----------
        bucket_name: str
            Name of the bucket
        source_blob_name: str
            Name of the blob to download
        folder_path: str
            Optional, the path of the blob in the bucket

        Returns
        -------
        str
            The content of the blob as a string, or None if an error occurs.
        """
        try:
            blob_path = folder_path + source_blob_name if folder_path else source_blob_name
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)

            blob_content = blob.download_as_bytes()
            blob_string = blob_content.decode("utf-8")

            logger.success(f"Downloaded storage object {source_blob_name} from bucket {bucket_name} as string.")
            return blob_string
        except Exception as e:
            logger.error(f"Error when downloading file: {e}")
            return None         
            
    @staticmethod        
    def list_blobs(bucket_name: str) -> list:
        """
        Lists all the blobs in the bucket.
        
        Parameters
        ----------
        bucket_name: str
            Name of the bucket
            
        Returns
        -------
        list
            List of blob names in the bucket
        """
        try:
            blobs_list = []
            storage_client = storage.Client()
            blobs = storage_client.list_blobs(bucket_name)
            for blob in blobs:
                blobs_list.append(blob.name)
            return blobs_list
        except Exception as e:
            logger.error(f"Error when listing blobs: {e}")
            return []
import ssl
import tempfile
import pg8000.dbapi
from loguru import logger

class PostgresUtils:
    def __init__(self):
        pass
    
    def _generate_temp_pem_file(
        self,
        value:str
    ) -> str:
        """
        Generate a temp file from a string value.
        Used to generate .PEM files used in SSL connection on Postgres
        
        Parameters
        ----------
        value: str
            The string value that will be stored in a temp file
        
        Returns
        -------
        temp_file.name: str
            The path as string of the temp file created
        """
        try:
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file.write(value.encode())
                temp_file.flush()  
            return temp_file.name
        except Exception as e:
            logger.error(f'Failed to generate temp file: {e}')
    
    def _generate_ssl_args( 
        self,
        db_root_cert: str, 
        db_cert: str, 
        db_key: str
    ) -> dict:
        
        """
        Generate the SSL dict context to create a secured SSL connection with Postgres

        Parameters
        ----------
        db_root_cert: str
            The path of the database root certificate
        db_cert: str
            The path of the database certificate
        db_key: str
            The path of the private key
                
        Returns
        -------
        connect_args: dict 
            The dict containing ssl context elements. Can be used as **connect_args
        """
        connect_args = {}
        
        try:            
            ssl_context = ssl.SSLContext()
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            ssl_context.load_verify_locations(db_root_cert)
            ssl_context.load_cert_chain(db_cert, db_key)
            connect_args["ssl_context"] = ssl_context
            return connect_args
        except Exception as e:
            logger.error(f'Failed to verify SSL elements: {e}')
    
    def connect_with_ssl(
        self,
        db_host: str,
        db_user: str,
        db_password: str,
        db_name: str,
        db_port: str,
        db_root_cert: str,
        db_cert: str,
        db_key: str
    ) -> pg8000.dbapi.Connection:
        """
        Create a SSL secured connection with Postgres Cloud SQL.
        The port will be converted as integer.
        
        Parameters
        ----------
        db_host: str
            The database host
        db_user: str
            The database username
        db_password: str
            The database username password
        db_name: str
            The name of the database
        db_port: str
            The port of the database
        db_root_cert: str
            The value of SSL root (server) certificate
        db_cert: str
            The value of SSL certificate
        db_key: str
            The value of SSL private key
        
        Returns
        -------
        connection: pg8000.dbapi.Connection
            The connection that will be used to interact with Postgres instance
        """
        
        db_root_cert = self._generate_temp_pem_file(db_root_cert)
        db_cert = self._generate_temp_pem_file(db_cert)
        db_key = self._generate_temp_pem_file(db_key)
        connect_args = self._generate_ssl_args(db_root_cert, db_cert, db_key)
        db_port = int(db_port)
        
        try:
            connection = pg8000.dbapi.connect(
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
                database=db_name,
                **connect_args
            )
            
            logger.success(f'Connection successfully established with {db_name}')
            return connection
        except Exception as e:
            logger.error(f'Failed to establish connection: {e}')
            raise e      
     
    def create_tables(self, connection):
        cursor = connection.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_information (
            id SERIAL PRIMARY KEY,
            date DATE,
            job_scraped VARCHAR(255),
            job_name VARCHAR(255),
            company_name VARCHAR(255),
            job_location VARCHAR(255),
            job_level VARCHAR(255),
            job_type VARCHAR(255),
            job_category VARCHAR(255),
            job_sector VARCHAR(255),
            job_id VARCHAR(255) UNIQUE
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs_description (
            id SERIAL PRIMARY KEY,
            job_information_id INTEGER UNIQUE REFERENCES jobs_information(id),
            description TEXT
        )
        """)
        
        connection.commit()
        cursor.close()
        logger.info("Database tables created successfully")
            
    def insert_job_data(
        self,
        connection: pg8000.dbapi.Connection,
        job_data: dict
    ) -> int:
        """
        Insert a job into the database using two tables:
        - jobs_information: Contains all job info except the description
        - jobs_description: Contains the job description linked to jobs_information
        
        Uses a transaction to ensure data consistency.
        
        Parameters
        ----------
        connection: pg8000.dbapi.Connection
            An active database connection
        job_data: dict
            Dictionary containing all job data fields
        
        Returns
        -------
        int
            The ID of the inserted job record
        """
        try:
            # Start a transaction
            cursor = connection.cursor()
            connection.autocommit = False
            
            # 1. Insert into jobs_information
            cursor.execute("""
                INSERT INTO jobs_information
                (date, job_scraped, job_name, company_name, job_location, 
                job_level, job_type, job_category, job_sector, job_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, 
                (job_data['date'], job_data['job_scraped'], job_data['job_name'],
                job_data['company_name'], job_data['job_location'], job_data['job_level'],
                job_data['job_type'], job_data['job_category'], 
                job_data['job_sector'], job_data['job_id'])
            )
            
            # Get the generated ID
            job_info_id = cursor.fetchone()[0]
            
            # 2. Insert into jobs_description with the reference
            cursor.execute("""
                INSERT INTO jobs_description
                (job_information_id, description)
                VALUES (%s, %s)
                """,
                (job_info_id, job_data['job_description'])
            )
            
            # Commit the transaction
            connection.commit()
            cursor.close()
            
            logger.info(f"Job successfully inserted with ID: {job_info_id}")
            return job_info_id
            
        except Exception as e:
            # Rollback the transaction in case of error
            connection.rollback()
            logger.error(f"Failed to insert job data: {e}")
            raise e
        
    def close_connection(self, connection: pg8000.dbapi.Connection) -> None:
        """
        Close the connection to the Postgres database.
        
        Parameters
        ----------
        connection: pg8000.dbapi.Connection
            The connection object to the Postgres database
        
        Returns
        -------
        None
        """
        try:
            if connection is not None:
                connection.close()
                logger.success('Connection successfully closed.')
        except Exception as e:
            logger.error(f'Failed to close connection: {e}')
            raise e
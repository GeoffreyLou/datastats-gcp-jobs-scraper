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
     
    def create_table_if_not_exists(
        self, 
        connection: pg8000.dbapi.Connection, 
        table_name: str, 
        table_schema: dict
    ) -> None:
        """
        Create a table in the Postgres database if it does not already exist.

        Parameters
        ----------
        connection: pg8000.dbapi.Connection
            The connection object to the Postgres database
        table_name: str
            The name of the table to create
        table_schema: dict
            A dictionary where keys are column names and values are column definitions (e.g., "id": "SERIAL PRIMARY KEY")

        Returns
        -------
        None
        """
        try:
            # Construct the SQL statement for creating the table
            columns_def = ", ".join([f"{col_name} {col_def}" for col_name, col_def in table_schema.items()])
            sql_statement = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"

            cursor = connection.cursor()
            cursor.execute(sql_statement)
            connection.commit()
            cursor.close()
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            raise e     
       
    def insert_data(
        self, 
        connection: pg8000.dbapi.Connection, 
        table_name: str, 
        data: dict
    ) -> None:
        """
        Insert data into a table in the Postgres database.

        Parameters
        ----------
        connection: pg8000.dbapi.Connection
            The connection object to the Postgres database
        table_name: str
            The name of the table to insert data into
        data: dict
            A dictionary where keys are column names and values are the data to insert

        Returns
        -------
        None
        """
        
        # Start a transaction
        connection.autocommit = False
        
        try:
            # Construct the SQL statement for inserting data
            columns = ", ".join(data.keys())
            
            # Prepare placeholders and values for parameterized query to prevent SQL injection
            placeholders = ", ".join(["%s"] * len(data))
            values = list(data.values())
            
            # Execute the query with RETURNING id to get the inserted row ID
            cursor = connection.cursor()
            cursor.execute(
                f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) RETURNING id", 
                values
            )
            
            # Get the ID of the inserted row
            row_id = cursor.fetchone()[0]
            connection.commit()
            return row_id

        except Exception as e:
            connection.rollback()
            logger.error(f"Failed to insert data into '{table_name}': {e}")
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
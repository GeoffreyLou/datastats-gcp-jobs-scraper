import os
from loguru import logger
from dotenv import load_dotenv
from dataclasses import dataclass

load_dotenv()

@dataclass
class Config:
    """
    dataclass used to create config from environment variables.
    
    If a new environment variable is added to the container, add it here.
    Do not forget to specify the type. 
    """
    DATASTATS_BUCKET_URLS: str
    DATASTATS_BUCKET_ARCHIVE: str
    DB_NAME: str
    DB_USER: str
    DB_PORT: str
    DB_HOST: str
    DB_ROOT_CERT: str
    DB_CERT: str
    DB_KEY: str
    DB_USER_PASSWORD: str

    @classmethod
    def load(cls) -> 'Config':
        """
        Load the environment variables to generate the config. 
        
        Returns
        -------
        Config
            An instance of Config with environment variables callable with their name
        
        Raises
        ------
        EnvironmentError
            If one or more environment variable is missing 
        
        """
        logger.info('Generating config from environment variables...')
        env_vars = {
            key: os.getenv(key)
            for key in cls.__annotations__.keys()
        }

        missing_vars = [key for key, value in env_vars.items() if value is None]
        if missing_vars:
            raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}")

        return cls(**env_vars)

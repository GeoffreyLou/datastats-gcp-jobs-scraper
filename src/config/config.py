import json
from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


project_root = Path(__file__).parent.parent.parent
config_dir: Path = project_root / "src" / "config"


# ------------------------------------------------------------------------------------------------------------------
# Functions to load schemas from json files
# ------------------------------------------------------------------------------------------------------------------    

def load_jobs_information_schema():
    try:
        return json.loads((config_dir / "jobs_information.json").read_text())
    except Exception as e:
        raise RuntimeError(f"Error loading jobs information schema: {e}")

def load_jobs_description_schema():
    try:
        return json.loads((config_dir / "jobs_description.json").read_text())
    except Exception as e:
        raise RuntimeError(f"Error loading jobs description schema: {e}")

def load_errors_schema():
    """
    Reads a json and load as dict for errors schema.
    """
    try:
        return json.loads((config_dir / "scrap_errors.json").read_text())
    except Exception as e:
        raise RuntimeError(f"Error loading errors schema: {e}")


# ------------------------------------------------------------------------------------------------------------------
# Generate config
# ------------------------------------------------------------------------------------------------------------------   

class Config(BaseSettings):
    """
    Configuration class for the application.
    Loads settings from .env file in dev and os in prod.
    """
    
    PROJECT_ID: str
    BQ_DATASET: str
    DATASTATS_BUCKET_URLS: str
    DATASTATS_BUCKET_ARCHIVE: str
    JOBS_INFORMATION_TABLE_NAME: str
    JOBS_DESCRIPTION_TABLE_NAME: str
    ERRORS_TABLE_NAME: str
    JOBS_INFORMATION_CONFIG: dict = Field(default_factory=load_jobs_information_schema)
    JOBS_DESCRIPTION_CONFIG: dict = Field(default_factory=load_jobs_description_schema)
    ERRORS_CONFIG: dict = Field(default_factory=load_errors_schema)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


# ------------------------------------------------------------------------------------------------------------------
# Load the config
# ------------------------------------------------------------------------------------------------------------------  

try:
    config = Config()
except Exception as e:
    print(f"Config Error: {e}")
    raise
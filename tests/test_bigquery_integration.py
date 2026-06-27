"""
Integration test for the jobs-scraper BigQuery switch: runs the real
BigQueryTableManager and DataStats table-creation/insert logic against the
BigQuery emulator. Bypasses __scrap_urls (GCS + HTTP scraping, unrelated to
this migration) by feeding fake scraped job dicts directly into
__insert_jobs_data, exactly as __scrap_urls would have produced them.

Requires the BigQuery emulator running:
    docker run -d -p 9050:9050 -p 9060:9060 ghcr.io/goccy/bigquery-emulator:latest --project=test-project

Then:
    uv run pytest tests/test_bigquery_integration.py -v
"""
import sys
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from google.auth.credentials import AnonymousCredentials
from google.api_core.client_options import ClientOptions
from google.cloud import bigquery

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.bigquery_utils import BigQueryTableManager  # noqa: E402
from utils.datastats_utils import DataStats  # noqa: E402

PROJECT_ID = "test-project"
DATASET_ID = "datastats_raw_jobs_test"
CONFIG_DIR = Path(__file__).parent.parent / "src" / "config"


@pytest.fixture(scope="module")
def bq_client():
    return bigquery.Client(
        project=PROJECT_ID,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:9050"),
    )


@pytest.fixture
def datastats(bq_client):
    with patch("utils.datastats_utils.BigQueryTableManager") as MockManagerClass:
        real_manager = BigQueryTableManager(project_id=PROJECT_ID, dataset_id=DATASET_ID, client=bq_client)
        MockManagerClass.return_value = real_manager

        instance = DataStats(
            project_id=PROJECT_ID,
            bq_dataset=DATASET_ID,
            urls_bucket_name="fake-urls-bucket",
            archive_bucket_name="fake-archive-bucket",
            jobs_information_table_name="jobs_information",
            jobs_information_config=json.loads((CONFIG_DIR / "jobs_information.json").read_text()),
            jobs_description_table_name="jobs_description",
            jobs_description_config=json.loads((CONFIG_DIR / "jobs_description.json").read_text()),
            scrap_errors_table_name="scrap_errors",
            scrap_errors_config=json.loads((CONFIG_DIR / "scrap_errors.json").read_text()),
        )
        yield instance


def test_create_tables_and_insert_jobs_data(datastats, bq_client):
    datastats._DataStats__create_tables()

    fake_jobs = [
        {
            "url": "https://example.com/jobs/1",
            "id_deduplication": "dedup-test-1",
            "scrap_date": "2025-06-01T00:00:00+00:00",
            "job_scraped": "data analyst",
            "job_name": "Data Analyst",
            "company_name": "Acme Corp",
            "location": "Paris",
            "level": "Junior",
            "type": "CDI",
            "category": "Data",
            "sector": "Tech",
            "description": "Description du poste Data Analyst chez Acme Corp.",
        },
        {
            "url": "https://example.com/jobs/2",
            "company_name": "ValueNotFound",
            "location": "ValueNotFound",
        },
    ]

    datastats._DataStats__insert_jobs_data(data_list=fake_jobs)

    info_rows = list(bq_client.query(
        f"SELECT id, id_deduplication, job_name, company_name FROM `{PROJECT_ID}.{DATASET_ID}.jobs_information`"
    ).result())
    assert len(info_rows) == 1
    assert info_rows[0].id_deduplication == "dedup-test-1"

    description_rows = list(bq_client.query(
        f"SELECT id_job_information, description FROM `{PROJECT_ID}.{DATASET_ID}.jobs_description`"
    ).result())
    assert len(description_rows) == 1
    # The description must be linked to the SAME generated id as the jobs_information row
    assert description_rows[0].id_job_information == info_rows[0].id
    assert "Acme Corp" in description_rows[0].description

    error_rows = list(bq_client.query(
        f"SELECT error_message, url FROM `{PROJECT_ID}.{DATASET_ID}.scrap_errors`"
    ).result())
    assert len(error_rows) == 1
    assert error_rows[0].url == "https://example.com/jobs/2"

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from typing import Dict, List, Optional
from loguru import logger


class BigQueryTableManager:
    """
    Gestionnaire pour la création et l'insertion de données dans BigQuery
    """

    def __init__(self, project_id: str, dataset_id: str, client: Optional[bigquery.Client] = None):
        self.client = client or bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id

    def create_dataset_if_not_exists(self):
        """Crée le dataset s'il n'existe pas"""
        dataset_id = f"{self.project_id}.{self.dataset_id}"

        try:
            self.client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "EU"
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_id}")

    def create_table_from_schema(
        self,
        table_name: str,
        schema: List[Dict],
        description: str = None,
        partition_field: str = None,
        clustering_fields: List[str] = None
    ):
        """
        Crée une table BigQuery à partir d'un schéma (idempotent)

        Parameters
        ----------
        table_name: str
            Nom de la table
        schema: List[Dict]
            Liste de dictionnaires définissant le schéma
        description: str
            Description de la table
        partition_field: str
            Champ pour le partitioning
        clustering_fields: List[str]
            Champs pour le clustering
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            self.client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
            return
        except NotFound:
            pass

        bq_schema = [
            bigquery.SchemaField(
                name=field['name'],
                field_type=field['type'],
                mode=field.get('mode', 'NULLABLE'),
                description=field.get('description', '')
            )
            for field in schema
        ]

        table = bigquery.Table(table_id, schema=bq_schema)

        if description:
            table.description = description

        if partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )

        if clustering_fields:
            table.clustering_fields = clustering_fields

        table = self.client.create_table(table)
        logger.info(f"Created table {table_id}")

    def insert_rows(self, table_name: str, rows: List[Dict]) -> None:
        """
        Insère une ou plusieurs lignes dans une table BigQuery

        Parameters
        ----------
        table_name: str
            Nom de la table cible (sans project/dataset)
        rows: List[Dict]
            Liste de dictionnaires représentant les lignes à insérer

        Returns
        -------
        None
        """
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        try:
            errors = self.client.insert_rows_json(table_id, rows)
            if errors:
                raise RuntimeError(f"Failed to insert rows into {table_id}: {errors}")
            logger.success(f"Inserted {len(rows)} row(s) into {table_id}")
        except Exception as e:
            logger.error(f"Failed to insert rows into '{table_id}': {e}")
            raise e

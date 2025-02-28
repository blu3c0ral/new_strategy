from typing import List, Optional, Union
from google.cloud import bigquery
import pandas as pd
import pandas_gbq

from persistence.persistence import PersistenceLayer


# Define the BigQuery client
class BigQueryPersistence(PersistenceLayer):
    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
    ):
        self._client = bigquery.Client(project=project_id)
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._table_id = table_id

    def insert_rows(
        self,
        rows: Union[dict, list, pd.DataFrame],
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
    ):
        """
        Inserts rows into a BigQuery table.

        :param rows: The rows to insert. Can be a dictionary, list of dictionaries, or a DataFrame. When using a dictionary or list of dictionaries, the keys must match the column names.
        :param project_id: The project ID.
        :param dataset_id: The dataset ID.
        :param table_id: The table ID.

        :return: None
        """
        project_id = project_id or self._project_id
        dataset_id = dataset_id or self._dataset_id
        table_id = table_id or self._table_id

        if not dataset_id or not table_id or not project_id:
            raise ValueError("Project ID, dataset ID and table ID are required")

        destination_table_id = f"{dataset_id}.{table_id}"
        full_table_id = f"{project_id}.{destination_table_id}.{table_id}"

        try:
            if isinstance(rows, pd.DataFrame):
                print("Inserting rows using pandas_gbq:")
                print(rows)
                pandas_gbq.to_gbq(
                    dataframe=rows,
                    destination_table=destination_table_id,
                    project_id=project_id,
                    if_exists="append",
                    progress_bar=True,
                )
                print("Rows inserted successfully.")
                return
            elif isinstance(rows, dict):
                errors = self._client.insert_rows_json(full_table_id, [rows])
                if errors:
                    raise Exception(f"Errors: {errors}")
            elif isinstance(rows, list):
                errors = self._client.insert_rows_json(full_table_id, rows)
                if errors:
                    raise Exception(f"Errors: {errors}")
        except Exception as e:
            print(f"Failed to insert rows: {e}")

    def save_data(self, data: Union[List[dict], pd.DataFrame]):
        """
        Save data to the BigQuery table.

        :param data: The data to save. Can be a list of dictionaries or DataFrames.
        """
        self.insert_rows(data)

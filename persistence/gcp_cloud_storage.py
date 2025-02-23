import csv
from io import StringIO
import json
from typing import List
from google.cloud import storage

from definitions import TickerRecord
from persistence.persistence import MAX_FILE_SIZE_DEFAULT, PersistenceLayer


class GCSPersistence(PersistenceLayer):
    """Implements Google Cloud Storage (GCS) logging with append support."""

    def __init__(
        self,
        bucket_name: str,
        gcs_prefix: str = "price_logs",
        format: str = "json",
        max_file_size: float = MAX_FILE_SIZE_DEFAULT,
    ):
        """
        Initializes GCS persistence layer.

        Args:
            bucket_name (str): Name of the GCS bucket.
            gcs_prefix (str): Prefix path in GCS (folder-like structure).
            format (str): Storage format - "json" or "csv".
        """
        super().__init__(max_file_size)

        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(bucket_name)
        self.gcs_prefix = gcs_prefix

        if format not in ["json", "csv"]:
            raise ValueError("Invalid format. Must be 'json' or 'csv'.")
        self.format = format

    def save_ticker_record(self, data: List[TickerRecord]):
        """
        Append price data to GCS without overwriting existing content.

        Args:
            data (List[TickerRecord]): List of TickerRecord objects.
        """
        timestamp = data[0].timestamp.split(" ")[
            0
        ]  # Use date (YYYY-MM-DD) for filename
        file_name = f"{self.gcs_prefix}/{timestamp}.{self.format}"

        if self.format == "json":
            self._append_json(file_name, data)
        elif self.format == "csv":
            self._append_csv(file_name, data)

    def _append_json(self, file_name: str, data: List[TickerRecord]):
        """Append new data to an existing JSON file or create a new one."""
        blob = self.bucket.blob(file_name)

        # Try to download existing JSON data
        try:
            existing_data = json.loads(blob.download_as_text())
        except Exception:
            existing_data = []  # File does not exist or is empty

        # Append new records as dictionaries
        new_data = existing_data + [record._asdict() for record in data]

        # Upload back to GCS
        blob.upload_from_string(
            json.dumps(new_data, indent=4), content_type="application/json"
        )
        print(f"Appended JSON data to GCS: gs://{self.bucket.name}/{file_name}")

    def _append_csv(self, file_name: str, data: List[TickerRecord]):
        """Append new data to an existing CSV file or create a new one."""
        blob = self.bucket.blob(file_name)
        existing_data = ""

        # Try to download existing CSV data
        try:
            existing_data = blob.download_as_text()
        except Exception:
            print(f"File {file_name} does not exist or is empty.")

        # Prepare CSV buffer
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write header only if it's a new file
        if not existing_data:
            csv_writer.writerow(["Timestamp", "Symbol", "Bid", "Ask", "Last", "Volume"])

        # Append new records
        for price in data:
            csv_writer.writerow(
                [
                    price.timestamp,
                    price.symbol,
                    price.bid,
                    price.ask,
                    price.last,
                    price.volume,
                ]
            )

        # Upload back to GCS
        blob.upload_from_string(
            existing_data + csv_buffer.getvalue(), content_type="text/csv"
        )
        print(f"Appended CSV data to GCS: gs://{self.bucket.name}/{file_name}")

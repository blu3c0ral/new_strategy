import csv
from datetime import datetime, time
from io import StringIO
import json
from typing import List, NamedTuple
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
import pytz

from definitions import TickerRecord
from persistence.persistence import MAX_FILE_SIZE_DEFAULT, PersistenceLayer


def get_bucket_size(bucket_name):
    """
    Calculate the total size of all objects in a GCS bucket.

    Args:
        bucket_name (str): Name of the GCS bucket

    Returns:
        tuple: (size_bytes, size_human_readable)
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.get_bucket(bucket_name)

        # List all blobs in the bucket
        blobs = bucket.list_blobs()

        # Calculate total size
        total_size_bytes = 0
        for blob in blobs:
            total_size_bytes += blob.size

        # Convert to human-readable format
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        size = float(total_size_bytes)
        unit_index = 0

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        size_human_readable = f"{size:.2f} {units[unit_index]}"

        return total_size_bytes, size_human_readable

    except GoogleAPIError as e:
        print(f"Error accessing bucket {bucket_name}: {e}")
        return None, None


def recursive_asdict(obj):
    """Recursively converts NamedTuple instances into dictionaries."""
    if hasattr(obj, "_asdict"):  # If obj is a NamedTuple
        return {key: recursive_asdict(value) for key, value in obj._asdict().items()}
    elif isinstance(obj, list):  # If obj is a list, process each item
        return [recursive_asdict(item) for item in obj]
    else:
        return obj  # Return as-is if not a NamedTuple or list


class GCSPersistence(PersistenceLayer):
    """Implements Google Cloud Storage (GCS) logging with append support."""

    def __init__(
        self,
        bucket_name: str,
        filename: str,
        gcs_prefix: str = "price_logs",
        format: str = "json",
        max_file_size: float = MAX_FILE_SIZE_DEFAULT,
        file_per_day: bool = False,
        tzinfo=pytz.timezone("US/Eastern"),
        new_date_hour: time = time(
            0, 0, tzinfo=pytz.timezone("US/Eastern")
        ),  # (24-hour format) - Not implemented yet!
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

        self._file_per_day = file_per_day
        self._new_date_hour = new_date_hour
        self._tzinfo = tzinfo

        self._base_filename = filename

        self.filename = (
            gcs_prefix
            + "/"
            + self._base_filename
            + (
                f"_{datetime.now(tz=self._tzinfo).date().isoformat()}"
                if self._file_per_day
                else ""
            )
            + f".{self.format}"
        )

    def save_ticker_records(self, data: List[TickerRecord]):
        """
        Append price data to GCS without overwriting existing content.

        Args:
            data (List[TickerRecord]): List of TickerRecord objects.
        """

        if self.format == "json":
            self._append_json(data)
        elif self.format == "csv":
            self._append_csv(data)

    def _append_json(self, data: List[NamedTuple]):
        """Append new data to an existing JSON file or create a new one."""
        blob = self.bucket.blob(self.filename)

        # Try to download existing JSON data
        try:
            existing_data = json.loads(blob.download_as_text())
        except Exception:
            existing_data = []  # File does not exist or is empty

        # print(json.dumps(recursive_asdict(data[0])))

        # Append new records as dictionaries
        new_data = existing_data + [recursive_asdict(record) for record in data]

        # print(new_data)

        # Upload back to GCS
        blob.upload_from_string(json.dumps(new_data), content_type="application/json")
        print(
            f"{datetime.now().isoformat()}\tAppended JSON data to GCS: gs://{self.bucket.name}/{self.filename}"
        )

    def _append_csv(self, data: List[TickerRecord]):
        """Append new data to an existing CSV file or create a new one."""
        blob = self.bucket.blob(self.filename)
        existing_data = ""

        # Try to download existing CSV data
        try:
            existing_data = blob.download_as_text()
        except Exception:
            print(f"File {self.filename} does not exist or is empty.")

        # Prepare CSV buffer
        csv_buffer = StringIO()
        csv_writer = csv.writer(csv_buffer)

        # Write header only if it's a new file
        if not existing_data:
            csv_writer.writerow(
                [
                    "timestamp",
                    "symbol",
                    "bid",
                    "bid_size",
                    "ask",
                    "ask_size",
                    "last",
                    "last_size",
                    "volume",
                    "open",
                    "high",
                    "low",
                    "close",
                ]
            )

        # Append new records
        for price in data:
            csv_writer.writerow(
                [
                    price.timestamp,
                    price.symbol,
                    price.bid,
                    price.bid_size,
                    price.ask,
                    price.ask_size,
                    price.last,
                    price.last_size,
                    price.volume,
                    price.open,
                    price.high,
                    price.low,
                    price.close,
                ]
            )

        # Upload back to GCS
        blob.upload_from_string(
            existing_data + csv_buffer.getvalue(), content_type="text/csv"
        )
        print(f"Appended CSV data to GCS: gs://{self.bucket.name}/{self.filename}")

    def _rotate_files(self):
        """Rotate files if they exceed the maximum file size."""

        # Check file date
        if self._file_per_day:
            now_date = datetime.now(tz=self._tzinfo).date()
            # Extract date from the filename e.g. "price_logs/price_data_2021-01-01.json" or "price_logs/price_data_2021-01-01.json_0"
            try:
                date_str = self.filename.split("_")[-1].split(".")[0]
                file_date = datetime.fromisoformat(date_str).date()
            except Exception:
                date_str = self.filename.split("_")[-2].split(".")[0]
                file_date = datetime.fromisoformat(date_str).date()
            # Check if the file date is different from the current date
            if file_date != now_date:
                # Update the filename with the new date
                self.filename = (
                    self.gcs_prefix
                    + "/"
                    + self._base_filename
                    + f"_{now_date.isoformat()}"
                    + f".{self.format}"
                )

        blob = self.bucket.blob(self.filename)
        blob.reload()
        if blob.size >= self.max_file_size:
            # check if the current filename ends with ".csv_<number>"
            if ".csv_" in self.filename:
                # The new filename should be with <number> incremented by 1
                parts = self.filename.split(".csv_")
                new_filename = f"{parts[0]}.csv_{int(parts[1]) + 1}"
            else:
                # Move current file to a new filename with "_0" appended
                # Create a new file with the number 1
                new_filename = f"{self.filename}_1"

                # Copy the current file to the new filename
                new_blob = self.bucket.blob(self.filename + "_0")
                new_blob.upload_from_string(blob.download_as_string())

                # Remove the current file
                blob.delete()

            print(
                f"Rotated GCS file: gs://{self.bucket.name}/{self.filename} -> gs://{self.bucket.name}/{new_filename}"
            )

            self.filename = new_filename

    def get_bucket_size(self):
        """
        Calculate the total size of all objects in the GCS bucket.

        Returns:
            tuple: (size_bytes, size_human_readable)
        """
        return get_bucket_size(self.bucket.name)

    def _get_all_buckets_size():
        """
        Calculate the total size of all objects in all GCS buckets.

        Returns:
            dict: {bucket_name: (size_bytes, size_human_readable)}
        """
        try:
            # Initialize the GCS client
            storage_client = storage.Client()

            # Get all buckets
            buckets = storage_client.list_buckets()

            # Calculate total size for each bucket
            bucket_sizes = {}
            for bucket in buckets:
                total_size_bytes, size_human_readable = get_bucket_size(bucket.name)
                bucket_sizes[bucket.name] = (total_size_bytes, size_human_readable)

            total_size = sum(size[0] for size in bucket_sizes.values())

            return bucket_sizes

        except GoogleAPIError as e:
            print(f"Error accessing GCS buckets: {e}")
            return None


if __name__ == "__main__":
    # Test the GCS persistence layer
    gcs_persistence = GCSPersistence(
        bucket_name="alpaca_intraday_data",
        filename="price_data",
        format="json",
        max_file_size=1e6,
        file_per_day=True,
    )

    # Test getting the bucket size
    print(gcs_persistence.get_bucket_size())

    # Test getting the size of all buckets
    print(GCSPersistence._get_all_buckets_size())

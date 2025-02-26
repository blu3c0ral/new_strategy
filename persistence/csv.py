import csv
import datetime
import os
import shutil

from typing import List

from definitions import TickerRecord
from persistence.persistence import MAX_FILE_SIZE_DEFAULT, PersistenceLayer


class CSVPersistence(PersistenceLayer):
    """Implements CSV logging."""

    def __init__(
        self,
        filename: str = "intraday_prices.csv",
        max_file_size: float = MAX_FILE_SIZE_DEFAULT,
    ):
        super().__init__(max_file_size)
        # Check if the filename ends with ".csv"
        if not filename.endswith(".csv"):
            raise ValueError("Invalid filename. Must end with '.csv'.")

        self.filename = filename
        # Check if the file exists. If yes, skip writing the header.
        try:
            with open(self.filename, "r") as csvfile:
                pass
        except FileNotFoundError:
            with open(self.filename, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
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

    def save_ticker_records(self, data: List[TickerRecord]):
        """Append price data to a CSV file."""
        with open(self.filename, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for price in data:
                writer.writerow(
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
        print(f"Saved to CSV: {self.filename}")

    def _rotate_files(self):
        """Rotate files if they exceed the maximum file size."""
        if os.path.getsize(self.filename) >= self.max_file_size:
            # check if the current filename ends with ".csv_<number>"
            if ".csv_" in self.filename:
                # The new filename should be with <number> incremented by 1
                parts = self.filename.split(".csv_")
                new_filename = f"{parts[0]}.csv_{int(parts[1]) + 1}"
            else:
                # The new filename should be with "_0" appended
                shutil.move(self.filename, f"{self.filename}_0")
                new_filename = f"{self.filename}_1"

            with open(new_filename, "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
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
            print(f"Rotated CSV file: {self.filename} -> {new_filename}")

            # Update the current filename
            self.filename = new_filename

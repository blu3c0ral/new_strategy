import csv
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
        self.filename = filename
        with open(self.filename, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["Timestamp", "Symbol", "Bid", "Ask", "Last", "Volume"])

    def save_ticker_record(self, data: List[TickerRecord]):
        """Append price data to a CSV file."""
        with open(self.filename, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for price in data:
                writer.writerow(
                    [
                        price.timestamp,
                        price.symbol,
                        price.bid,
                        price.ask,
                        price.last,
                        price.volume,
                    ]
                )
        print(f"Saved to CSV: {self.filename}")

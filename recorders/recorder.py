import time

from abc import ABC, abstractmethod
from typing import List, Dict

from definitions import TickerRecord
from persistence.persistence import PersistenceLayer


class TickerPriceLogger(ABC):
    """Abstract base class for different ticker price loggers."""

    def __init__(
        self, stocks: List[str], log_interval: int, persistences: List[PersistenceLayer]
    ):
        self._stocks = stocks
        self._log_interval = log_interval
        self._persistences = persistences

    @abstractmethod
    def connect(self):
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def _get_ticker_records(self) -> List[TickerRecord]:
        """Fetches the latest prices for the tracked stocks."""
        pass

    def _log_prices(self):
        """Fetch and log prices using all configured persistence layers."""
        prices = self._get_ticker_records()
        for persistence in self._persistences:
            persistence.save_ticker_records(prices)

    @abstractmethod
    def disconnect(self):
        """Close connection to the data source."""
        pass

    def run(self):
        """Continuously log prices at the specified interval."""
        try:
            while True:
                self._log_prices()
                time.sleep(self._log_interval)  # Convert minutes to seconds
        except KeyboardInterrupt:
            print("Stopping logger...")
            self.disconnect()

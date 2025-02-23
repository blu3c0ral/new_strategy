from abc import ABC, abstractmethod
from typing import List, Dict

from definitions import TickerRecord

MAX_FILE_SIZE_DEFAULT = 1e6  # 1 MB


class PersistenceLayer(ABC):
    """Abstract base class for different logging storage implementations."""

    def __init__(self, max_file_size: float = MAX_FILE_SIZE_DEFAULT):
        """Allows per-instance file size configuration."""
        self.max_file_size = max_file_size

    @abstractmethod
    def save_ticker_records(self, data: List[TickerRecord]):
        """Save price data to a storage backend (CSV, AWS S3, GCP, etc.)."""
        pass

from abc import ABC, abstractmethod
from typing import List, Dict, Union

import pandas as pd

from definitions import LoggerRecord

MAX_FILE_SIZE_DEFAULT = 25 * 1e6  # 25 * 1 MB


class PersistenceLayer(ABC):
    """Abstract base class for different logging storage implementations."""

    def __init__(self, max_file_size: float = MAX_FILE_SIZE_DEFAULT):
        """Allows per-instance file size configuration."""
        self.max_file_size = max_file_size

    # @abstractmethod
    # def save_ticker_records(self, data: List[LoggerRecord]):
    #     """Save price data to a storage backend (CSV, AWS S3, GCP, etc.)."""
    #     self.save_data(data)

    def _rotate_files(self):
        """Rotate files if they exceed the maximum file size."""
        pass

    @abstractmethod
    def save_data(self, data: Union[List[any], pd.DataFrame]):
        """Save price data to a storage backend (CSV, AWS S3, GCP, etc.)."""
        pass

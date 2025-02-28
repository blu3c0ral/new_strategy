import time

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union
import datetime

from definitions import (
    EST_TRADING_SESSION_LOGGER_TIMINGS,
    LoggerRecord,
    LoggerTiming,
    TickerRecord,
)
from persistence.persistence import PersistenceLayer


class MarketRecordsLogger(ABC):
    """Abstract base class for different ticker price loggers."""

    def __init__(
        self,
        stocks: List[str],
        persistences: List[PersistenceLayer],
        log_intervals: Optional[
            Union[int, List[LoggerTiming]]
        ] = EST_TRADING_SESSION_LOGGER_TIMINGS,
        default_timing: int = 1 * 60,  # Seconds
        hours_in_day: Optional[
            List[datetime.time]
        ] = None,  # List of times to log data. By the minutes (seconds are ignored)
    ):
        self._stocks = stocks
        self._log_intervals = log_intervals
        self._log_interval = default_timing
        self._hours_in_day = (
            [time.replace(second=0) for time in hours_in_day]
            if hours_in_day is not None
            else None
        )
        self._persistences = persistences

    @abstractmethod
    def connect(self):
        """Establish a connection to the data source."""
        pass

    @abstractmethod
    def _get_records(self) -> List[LoggerRecord]:
        """Fetches the latest prices for the tracked stocks."""
        pass

    def _log_records(self):
        """Fetch and log prices using all configured persistence layers."""
        prices = self._get_records()
        for persistence in self._persistences:
            persistence.save_data(prices)

    def _rotate_files(self):
        """Rotate files in all configured persistence layers."""
        for persistence in self._persistences:
            persistence._rotate_files()

    @abstractmethod
    def disconnect(self):
        """Close connection to the data source."""
        pass

    def run(self):
        """Continuously log prices at the specified interval."""
        # Try to connect to the data source
        try:
            self.connect()
        except Exception as e:
            print(f"Failed to connect: {e}")

        try:
            while True:
                if self._hours_in_day is None:
                    if self._log_intervals is not None:
                        if isinstance(self._log_intervals, int):
                            time.sleep(self._log_intervals)
                        else:
                            slept = False
                            for timing in self._log_intervals:
                                if timing.is_logging_time():
                                    time.sleep(timing.get_log_interval())
                                    slept = True
                                    break
                            if not slept:
                                time.sleep(self._log_interval)
                    else:
                        time.sleep(self._log_interval)

                    self._log_records()
                    self._rotate_files()
                else:
                    now = datetime.datetime.now().time()
                    time_now = datetime.time(hour=now.hour, minute=now.minute)
                    if time_now in self._hours_in_day:
                        print(f"Logging at {time_now}")

                        self._log_records()
                        self._rotate_files()

                        time.sleep(
                            60
                        )  # Sleep for a minute to avoid logging multiple times in the same minute

        except KeyboardInterrupt:
            print("Stopping logger...")
        finally:
            self.disconnect()
            print("Logger stopped.")

    def run_once(self):
        """Log prices once and stop."""
        try:
            self.connect()
        except Exception as e:
            print(f"Failed to connect: {e}")

        try:
            self._log_records()
            self._rotate_files()
        except Exception as e:
            print(f"Failed to log records: {e}")
        finally:
            self.disconnect()
            print("Logger stopped.")

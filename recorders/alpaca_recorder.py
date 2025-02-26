import time
from typing import List, Optional, Union

from brokerage_systems.alpaca_br.alpaca_defs import AlpacaSnapshot
from brokerage_systems.alpaca_br.alpaca_main import AlpacaClient
from definitions import EST_TRADING_SESSION_LOGGER_TIMINGS, LoggerTiming
from persistence.persistence import PersistenceLayer
from recorders.recorder import MarketRecordsLogger


class AlpacaSnapshotRecorder(MarketRecordsLogger):
    """
    Alpaca implementation of the MarketRecordsLogger interface
    """

    def __init__(
        self,
        stocks: List[str],
        config: dict,
        persistences: List[PersistenceLayer],
        log_intervals: Optional[
            Union[int, List[LoggerTiming]]
        ] = EST_TRADING_SESSION_LOGGER_TIMINGS,
        default_timing: int = 1 * 60,  # Seconds
    ):
        """
        Initialize the Alpaca snapshot logger.

        Args:
            stocks (List[str]): List of stock symbols to track.
            config (dict): Configuration for the Alpaca API.
            persistences (List[PersistenceLayer]): List of persistence layers to store data.
            log_intervals (Optional[Union[int, List[LoggerTiming]]]): List of timings to log data.
            default_timing (int): Default logging interval in seconds.
        """
        super().__init__(
            stocks=stocks,
            persistences=persistences,
            log_intervals=log_intervals,
            default_timing=default_timing,
        )

        self._alpaca = AlpacaClient(api_key=config["key"], secret_key=config["secret"])

    def connect(self):
        pass

    def _get_records(self) -> List[AlpacaSnapshot]:
        """
        Fetch the latest snapshots for tracked stocks.
        """
        snapshots = self._alpaca.get_snapshot(
            symbols=self._stocks,
            feed="iex",
        )

        snapshots_dict_list = [
            AlpacaSnapshot.from_dict(snapshot) for snapshot in snapshots.values()
        ]

        return snapshots_dict_list

    def disconnect(self):
        pass

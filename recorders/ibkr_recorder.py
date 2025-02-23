from typing import List
from ib_insync import *
import datetime
import csv
import time

from definitions import TickerRecord
from persistence.persistence import PersistenceLayer
from recorders.recorder import TickerPriceLogger


class IBKRTickerPriceLogger(TickerPriceLogger):
    """IBKR implementation of the TickerPriceLogger interface."""

    def __init__(
        self,
        stocks: List[str],
        log_interval: int = 60 * 5,
        persistences: List[PersistenceLayer] = [],
    ):
        """
        Initialize the IBKR ticker logger.

        Args:
            stocks (List[str]): List of stock symbols to track.
            log_interval (int): Interval in seconds to log prices.
            csv_filename (str): File to store logged prices.
        """
        super().__init__(stocks, log_interval, persistences)

        self._ib = IB()
        self._contracts = []

    def connect(self):
        """Connect to IBKR TWS or IB Gateway."""
        print("Connecting to IBKR...")
        self._ib.connect("127.0.0.1", 7497, clientId=1)
        self._contracts = [
            self._ib.qualifyContracts(Stock(symbol, "SMART", "USD"))[0]
            for symbol in self._stocks
        ]
        print("Connected to IBKR.")

    def _get_ticker_records(self) -> List[TickerRecord]:
        """Fetch the latest prices for tracked stocks."""
        tickers = self._ib.reqMktData(self._contracts)
        time.sleep(2)  # Allow time for data to populate

        prices = []
        for ticker in tickers:
            prices.append(
                TickerRecord(
                    timestamp=(
                        ticker.timestamp
                        if hasattr(ticker, "timestamp")
                        else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    ),
                    symbol=ticker.contract.symbol,
                    bid=ticker.bid if hasattr(ticker, "bid") else 0,
                    ask=ticker.ask if hasattr(ticker, "ask") else 0,
                    last=ticker.last if hasattr(ticker, "last") else 0,
                    volume=ticker.volume if hasattr(ticker, "volume") else 0,
                )
            )
        return prices

    def disconnect(self):
        """Disconnect from IBKR."""
        self._ib.disconnect()
        print("Disconnected from IBKR.")

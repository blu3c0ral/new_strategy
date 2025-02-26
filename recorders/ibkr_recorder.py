from typing import List
from ib_insync import *
import datetime
import csv
import time

from definitions import TickerRecord
from persistence.persistence import PersistenceLayer
from recorders.recorder import MarketRecordsLogger


class IBKRTickerPriceLogger(MarketRecordsLogger):
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
        self._ib.connect("127.0.0.1", 4001, clientId=1)
        self._contracts = [
            self._ib.qualifyContracts(Stock(symbol, "SMART", "USD"))[0]
            for symbol in self._stocks
        ]
        print("Connected to IBKR.")

    def _get_records(self) -> List[TickerRecord]:
        """Fetch the latest prices for tracked stocks."""
        tickers = []
        for contract in self._contracts:
            tickers.append(self._ib.reqTickers(contract)[0])
        time.sleep(2)  # Allow time for data to populate

        prices = []
        for ticker in tickers:
            prices.append(
                TickerRecord(
                    timestamp=ticker.time.timestamp(),
                    symbol=ticker.contract.symbol,
                    bid=ticker.bid if hasattr(ticker, "bid") else 0,
                    bid_size=ticker.bidSize if hasattr(ticker, "bidSize") else 0,
                    ask=ticker.ask if hasattr(ticker, "ask") else 0,
                    ask_size=ticker.askSize if hasattr(ticker, "askSize") else 0,
                    last=ticker.last if hasattr(ticker, "last") else 0,
                    last_size=ticker.lastSize if hasattr(ticker, "lastSize") else 0,
                    volume=ticker.volume if type(ticker.volume) == int else 0,
                    open=ticker.open if hasattr(ticker, "open") else 0,
                    high=ticker.high if hasattr(ticker, "high") else 0,
                    low=ticker.low if hasattr(ticker, "low") else 0,
                    close=ticker.close if hasattr(ticker, "close") else 0,
                )
            )
        return prices

    def disconnect(self):
        """Disconnect from IBKR."""
        self._ib.disconnect()
        print("Disconnected from IBKR.")


if __name__ == "__main__":
    # from persistence.csv import CSVPersistence
    from persistence.gcp_cloud_storage import GCSPersistence

    pl = GCSPersistence("ibkr_intraday_data")

    ibkr_logger = IBKRTickerPriceLogger(
        stocks=["SPY", "VOO"], log_interval=30, persistences=[pl]
    )

    ibkr_logger.run()

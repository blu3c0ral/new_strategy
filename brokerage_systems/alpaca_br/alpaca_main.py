from datetime import timedelta
from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo
from alpaca.data.historical.stock import StockHistoricalDataClient

from alpaca.data.requests import StockSnapshotRequest

from .alpaca_defs import AlpacaSnapshot


def example():
    from .alpaca_defs import config

    ALPACA_API_KEY = config["key"]
    ALPACA_SECRET_KEY = config["secret"]

    data_api_url = None

    symbol = ["SPY", "AAPL"]

    stock_historical_data_client = StockHistoricalDataClient(
        ALPACA_API_KEY, ALPACA_SECRET_KEY, url_override=data_api_url
    )

    req = StockSnapshotRequest(
        symbol_or_symbols=symbol,
        feed="iex",
    )

    print(stock_historical_data_client.get_stock_snapshot(req))


class AlpacaClient:
    """A client for fetching stock market data from Alpaca."""

    def __init__(
        self,
        api_key: str,
        secret_key: str,
        data_api_url: Optional[str] = None,
        api_retries: int = 3,
    ):
        """
        Initializes the Alpaca API client.

        :param api_key: Alpaca API key.
        :param secret_key: Alpaca secret key.
        :param data_api_url: Optional URL override for the data API.
        """

        self._retries = api_retries

        for _ in range(self._retries):
            try:
                self._client = StockHistoricalDataClient(
                    api_key, secret_key, url_override=data_api_url
                )
                break
            except Exception as e:
                print(f"Failed to connect to Alpaca: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to connect to Alpaca.")

    def get_snapshot(self, symbols: List[str], feed: str = "iex") -> AlpacaSnapshot:
        """
        Fetches snapshot data for given stock symbols.

        :param symbols: List of stock symbols to retrieve snapshot data for.
        :param feed: The data feed source (default: "iex").
        :return: Snapshot data from Alpaca API.
        """
        for _ in range(self._retries):
            try:
                ssr = StockSnapshotRequest(symbol_or_symbols=symbols, feed=feed)
                return self._client.get_stock_snapshot(ssr)
            except Exception as e:
                print(f"Failed to fetch snapshot data: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to fetch snapshot data.")


if __name__ == "__main__":
    example()

from datetime import timedelta
from datetime import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo
from alpaca.data.historical.stock import StockHistoricalDataClient

from alpaca.data.requests import (
    StockSnapshotRequest,
    StockTradesRequest,
    StockLatestQuoteRequest,
)


def example_snapshots():
    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    data_api_url = None

    symbol = ["SPY"]

    stock_historical_data_client = StockHistoricalDataClient(
        ALPACA_API_KEY, ALPACA_SECRET_KEY, url_override=data_api_url, raw_data=False
    )

    req = StockSnapshotRequest(
        symbol_or_symbols=symbol,
        feed="iex",
    )

    print(stock_historical_data_client.get_stock_snapshot(req))


def get_trades_example():

    from alpaca.data.requests import (
        CorporateActionsRequest,
        StockBarsRequest,
        StockQuotesRequest,
        StockTradesRequest,
    )

    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    alpaca_config = get_config_from_env()

    stock_historical_data_client = StockHistoricalDataClient(
        alpaca_config["key"], alpaca_config["secret"], url_override=None
    )

    now = datetime.now(ZoneInfo("America/New_York"))

    symbols = ["SPY", "VOO"]

    # Start date: 9:00am on 24 of February 2025
    start = datetime(2025, 2, 22, 9, 0, 0, 0, ZoneInfo("America/New_York"))

    # End date: 4:00pm on 24 of February 2025
    end = datetime(2025, 2, 22, 16, 0, 0, 0, ZoneInfo("America/New_York"))

    tic = datetime.now()

    req = StockTradesRequest(
        symbol_or_symbols=[symbols[0]],
        feed="iex",
        start=start,
        end=end,
        # - timedelta(
        #     days=5
        # ),  # specify start datetime, default=the beginning of the current day.
        # end=None,                                             # specify end datetime, default=now
        # limit=2,  # specify limit
    )
    df = stock_historical_data_client.get_stock_trades(req).df

    toc = datetime.now()
    print(f"Time taken: {toc - tic}")

    # print the df
    print(df)


def get_bar_example():

    from alpaca.data.requests import (
        CorporateActionsRequest,
        StockBarsRequest,
        StockQuotesRequest,
        StockTradesRequest,
    )

    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    alpaca_config = get_config_from_env()

    stock_historical_data_client = StockHistoricalDataClient(
        alpaca_config["key"], alpaca_config["secret"], url_override=None
    )

    now = datetime.now(ZoneInfo("America/New_York"))

    symbols = ["SPY", "VOO"]

    # Start date: 9:00am on 24 of February 2025
    start = datetime(2022, 2, 25, 8, 55, 0, 0, ZoneInfo("America/New_York"))

    # End date: 4:00pm on 24 of February 2025
    end = datetime(2022, 2, 25, 16, 5, 0, 0, ZoneInfo("America/New_York"))

    tic = datetime.now()

    req = StockBarsRequest(
        symbol_or_symbols=[symbols[0]],
        feed="iex",
    )
    df = stock_historical_data_client.get_stock_trades(req).df

    toc = datetime.now()
    print(f"Time taken: {toc - tic}")

    # print the df
    print(df)


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

    def get_snapshot(self, symbols: List[str], feed: str = "iex"):
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

    def get_trades(
        self, symbols: List[str], start: datetime, end: datetime, feed: str = "iex"
    ):
        """
        Fetches trade data for given stock symbols.

        :param symbols: List of stock symbols to retrieve trade data for.
        :param
        :param feed: The data feed source (default: "iex").
        :return: Trade data from Alpaca API.
        """

        for _ in range(self._retries):
            try:
                req = StockTradesRequest(
                    symbol_or_symbols=symbols, feed=feed, start=start, end=end
                )
                return self._client.get_stock_trades(req).df
            except Exception as e:
                print(f"Failed to fetch trade data: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to fetch trade data.")

    def get_last_qoute(self, symbols: List[str], feed: str = "iex"):
        """
        Fetches last quote data for given stock symbols.

        :param symbols: List of stock symbols to retrieve last quote data for.
        :param feed: The data feed source (default: "iex").
        :return: Last quote data from Alpaca API.
        """

        for _ in range(self._retries):
            try:
                req = StockLatestQuoteRequest(symbol_or_symbols=symbols, feed=feed)
                return self._client.get_stock_quotes(req).df
            except Exception as e:
                print(f"Failed to fetch last quote data: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to fetch last quote data.")


if __name__ == "__main__":
    from .alpaca_defs import get_config_from_env

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    alpaca_client = AlpacaClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    # Get snapshot data
    symbols = ["SPY"]
    print(alpaca_client.get_snapshot(symbols))

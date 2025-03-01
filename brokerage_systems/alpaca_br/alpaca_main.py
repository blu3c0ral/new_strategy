from datetime import timedelta
from datetime import datetime
import json
from typing import List, Optional
from zoneinfo import ZoneInfo
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.historical.option import OptionHistoricalDataClient

from alpaca.data.requests import (
    StockSnapshotRequest,
    StockTradesRequest,
    StockLatestQuoteRequest,
    StockQuotesRequest,
    OptionChainRequest,
)

import pandas as pd


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


def recursive_asdict(obj):
    """Recursively converts NamedTuple instances into dictionaries."""
    if hasattr(obj, "_asdict"):  # If obj is a NamedTuple
        return {key: recursive_asdict(value) for key, value in obj._asdict().items()}
    elif isinstance(obj, list):  # If obj is a list, process each item
        return [recursive_asdict(item) for item in obj]
    else:
        return obj  # Return as-is if not a NamedTuple or list


def get_options_chain_examples():
    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    from alpaca.data.requests import OptionChainRequest

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    data_api_url = None

    symbol = "SPY"

    option_historical_data_client = OptionHistoricalDataClient(
        ALPACA_API_KEY, ALPACA_SECRET_KEY, url_override=data_api_url, raw_data=False
    )

    req = OptionChainRequest(
        underlying_symbol=symbol,
        # feed="indicative",
    )

    data = option_historical_data_client.get_option_chain(req)

    # Convert to JSON
    print(dir(data[list(data.keys())[0]]))

    json_data = data[list(data.keys())[0]].latest_quote

    # Save to a file
    with open("option_chain.json", "w") as f:
        f.write(json.dumps(json_data))


def get_options_latest_trade_examples():
    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    from alpaca.data.requests import (
        OptionChainRequest,
        OptionLatestTradeRequest,
        OptionLatestQuoteRequest,
    )

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    data_api_url = None

    symbol = "SPY250303C00593000"

    option_historical_data_client = OptionHistoricalDataClient(
        ALPACA_API_KEY, ALPACA_SECRET_KEY, url_override=data_api_url, raw_data=False
    )

    req = OptionLatestTradeRequest(
        symbol_or_symbols=symbol,
        # feed="indicative",
    )

    data = option_historical_data_client.get_option_latest_trade(req)

    # Convert to JSON
    print(data)


def get_options_latest_quote_examples():
    from .alpaca_defs import AlpacaSnapshot, get_config_from_env

    from alpaca.data.requests import (
        OptionChainRequest,
        OptionLatestTradeRequest,
        OptionLatestQuoteRequest,
        OptionSnapshotRequest,
    )

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    data_api_url = None

    symbol = "SPY250303C00593000"

    option_historical_data_client = OptionHistoricalDataClient(
        ALPACA_API_KEY, ALPACA_SECRET_KEY, url_override=data_api_url, raw_data=False
    )

    req = OptionSnapshotRequest(
        symbol_or_symbols=symbol,
        # feed="indicative",
    )

    data = option_historical_data_client.get_option_snapshot(req)

    # Convert to JSON
    print(data)


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
                self._option_client = OptionHistoricalDataClient(
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

    def get_qoutes(
        self, symbols: List[str], start: datetime, end: datetime, feed: str = "iex"
    ):
        """
        Fetches last quote data for given stock symbols.

        :param symbols: List of stock symbols to retrieve last quote data for.
        :param feed: The data feed source (default: "iex").
        :return: Last quote data from Alpaca API.
        """

        for _ in range(self._retries):
            try:
                req = StockQuotesRequest(symbol_or_symbols=symbols, feed=feed)
                return self._client.get_stock_quotes(req).df
            except Exception as e:
                print(f"Failed to fetch last quote data: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to fetch last quote data.")

    def get_option_chain(
        self, underlying_symbol: str, as_rows: bool = True, as_df: bool = True
    ):
        """
        Fetches option chain data for a given underlying symbol.

        :param underlying_symbol: The underlying symbol to retrieve option chain data for.
        :param as_rows: Whether to return the data as a list of rows (default: True).
        :param as_df: Whether to return the data as a DataFrame (default: True). Relevant only for as_rows=True.

        :return: Option chain data from Alpaca API.
        """

        data = {}

        for _ in range(self._retries):
            try:
                req = OptionChainRequest(underlying_symbol=underlying_symbol)
                data = self._option_client.get_option_chain(req)
                break
            except Exception as e:
                print(f"Failed to fetch option chain data: {e}")
                print("Retrying...")
                continue
        else:
            raise ConnectionError("Failed to fetch option chain data.")

        if not as_rows:
            return data

        # UTC time
        now = datetime.now(ZoneInfo("UTC"))

        symbols = list(data.keys())
        rows = []
        for symbol in symbols:
            json_obj = json.loads(data[symbol].latest_quote.model_dump_json())
            json_obj["implied_volatility"] = data[symbol].implied_volatility
            json_obj["delta"] = (
                data[symbol].greeks.delta if data[symbol].greeks else None
            )
            json_obj["gamma"] = (
                data[symbol].greeks.gamma if data[symbol].greeks else None
            )
            json_obj["theta"] = (
                data[symbol].greeks.theta if data[symbol].greeks else None
            )
            json_obj["vega"] = data[symbol].greeks.vega if data[symbol].greeks else None
            json_obj["rho"] = data[symbol].greeks.rho if data[symbol].greeks else None
            json_obj["insert_timestamp"] = now
            rows.append(json_obj)

        if as_df:
            df = pd.DataFrame(rows)

            # Correct timestamps columns types
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df["insert_timestamp"] = pd.to_datetime(df["insert_timestamp"], utc=True)

            return df

        return rows


if __name__ == "__main__":
    from .alpaca_defs import get_config_from_env

    alpaca_config = get_config_from_env()

    ALPACA_API_KEY = alpaca_config["key"]
    ALPACA_SECRET_KEY = alpaca_config["secret"]

    alpaca_client = AlpacaClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    # Get snapshot data
    symbol = "SPY"
    print(alpaca_client.get_option_chain(symbol))

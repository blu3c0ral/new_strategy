from datetime import datetime
import os
from typing import NamedTuple
from definitions import BarData, LoggerRecord, QuoteData, TradeData


class AlpacaSnapshot(NamedTuple):
    daily_bar: BarData
    latest_quote: QuoteData
    latest_trade: TradeData
    minute_bar: BarData
    previous_daily_bar: BarData
    symbol: str

    @staticmethod
    def _convert_timestamp(data: dict) -> dict:
        """Convert datetime to float timestamp (epoch format)."""
        if "timestamp" in data and isinstance(data["timestamp"], datetime):
            data["timestamp"] = data["timestamp"].timestamp()
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "AlpacaSnapshot":
        """Factory method to create an instance from a dictionary."""
        data = data.dict()
        return cls(
            daily_bar=BarData(**cls._convert_timestamp(data["daily_bar"])),
            latest_quote=QuoteData(**cls._convert_timestamp(data["latest_quote"])),
            latest_trade=TradeData(**cls._convert_timestamp(data["latest_trade"])),
            minute_bar=BarData(**cls._convert_timestamp(data["minute_bar"])),
            previous_daily_bar=BarData(
                **cls._convert_timestamp(data["previous_daily_bar"])
            ),
            symbol=data["symbol"],
        )


def get_config_from_env(key="ALPACA_KEY", secret="ALPACA_SECRET") -> dict:
    """Get the Alpaca API key and secret from environment variables."""
    return {
        "key": os.getenv(key),
        "secret": os.getenv(secret),
    }

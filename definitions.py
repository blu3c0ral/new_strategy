from datetime import datetime, time
from typing import List, NamedTuple

import pytz


LoggerRecord = NamedTuple


class TickerRecord(NamedTuple):
    timestamp: float
    symbol: str
    bid: float
    bid_size: float
    ask: float
    ask_size: float
    last: float
    last_size: float
    volume: int
    open: float
    high: float
    low: float
    close: float


class BarData(NamedTuple):
    close: float
    high: float
    low: float
    open: float
    symbol: str
    timestamp: float
    trade_count: float
    volume: float
    vwap: float


class QuoteData(NamedTuple):
    ask_exchange: str
    ask_price: float
    ask_size: float
    bid_exchange: str
    bid_price: float
    bid_size: float
    conditions: List[str]
    symbol: str
    tape: str
    timestamp: float


class TradeData(NamedTuple):
    conditions: List[str]
    exchange: str
    id: int
    price: float
    size: float
    symbol: str
    tape: str
    timestamp: float


# Class to enable different log timings regimes
class LoggerTiming:
    def __init__(
        self,
        start_time: time,
        end_time: time,
        log_interval: int,
        tzinfo=pytz.timezone("US/Eastern"),
    ):
        # Validate the input
        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")
        if log_interval <= 0:
            raise ValueError("log_interval must be positive")

        # Store the values
        self.start_time = start_time
        self.end_time = end_time
        self.log_interval = log_interval
        self.tzinfo = tzinfo

    def is_logging_time(self) -> bool:
        current_time = datetime.now(tz=self.tzinfo).time()
        start_time = self.start_time
        end_time = self.end_time
        return start_time <= current_time < end_time

    def get_log_interval(self) -> int:
        return self.log_interval


# The following is a list of logger timings fits the EST trading session
# (9:30 AM - 4:00 PM)
# The timings are in 24-hour format
# The log interval is in seconds
# Change intervals as needed
EST_TRADING_SESSION_LOGGER_TIMINGS = [
    LoggerTiming(time(9, 25), time(16, 5), 5),
]

from typing import NamedTuple


class TickerRecord(NamedTuple):
    timestamp: str
    symbol: str
    bid: float
    ask: float
    last: float
    volume: int

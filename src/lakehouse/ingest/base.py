from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

import pandas as pd

RAW_PRICE_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
]


class PriceSource(ABC):
    """A connector that extracts daily OHLCV bars for a date range (inclusive)."""

    name: str

    def __init__(self, securities: pd.DataFrame):
        self.securities = securities

    @abstractmethod
    def fetch(self, start: date, end: date) -> pd.DataFrame: ...

    def _finalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["source"] = self.name
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        return df.reindex(columns=RAW_PRICE_COLUMNS)

"""Deterministic synthetic market data.

Prices follow geometric Brownian motion seeded per (symbol, date), so any date
range always produces the same bars: re-running a backfill is reproducible and
incremental loads stitch together exactly. A small, controlled fraction of
records is deliberately corrupted so the data-quality layer has real work to do.
"""

from __future__ import annotations

import hashlib
from datetime import date

import numpy as np
import pandas as pd

from lakehouse.ingest.base import PriceSource

EPOCH = date(2020, 1, 1)
DEFECT_RATE = 0.004


def _seed(*parts: object) -> int:
    digest = hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()
    return int(digest[:12], 16)


class SyntheticSource(PriceSource):
    name = "synthetic"

    def __init__(self, securities: pd.DataFrame, inject_defects: bool = True):
        super().__init__(securities)
        self.inject_defects = inject_defects

    def fetch(self, start: date, end: date) -> pd.DataFrame:
        # Walk from a fixed epoch so a bar's price never depends on the requested range.
        days = pd.bdate_range(EPOCH, end)
        frames = []
        for symbol in self.securities["symbol"]:
            rng = np.random.default_rng(_seed("path", symbol))
            base = 200 + rng.random() * 3800
            drift, vol = rng.normal(0.0004, 0.0002), rng.uniform(0.012, 0.025)
            shocks = rng.normal(drift, vol, len(days))
            close = base * np.exp(np.cumsum(shocks))
            prev_close = np.concatenate([[base], close[:-1]])
            gap = rng.normal(0, vol / 3, len(days))
            open_ = prev_close * np.exp(gap)
            spread = np.abs(rng.normal(0, vol / 2, len(days)))
            high = np.maximum(open_, close) * (1 + spread)
            low = np.minimum(open_, close) * (1 - spread)
            volume = (rng.lognormal(14, 0.5, len(days))).astype("int64")
            frames.append(
                pd.DataFrame(
                    {
                        "symbol": symbol,
                        "trade_date": days.date,
                        "open": open_.round(2),
                        "high": high.round(2),
                        "low": low.round(2),
                        "close": close.round(2),
                        "volume": volume,
                    }
                )
            )
        df = pd.concat(frames, ignore_index=True)
        df = df[(df["trade_date"] >= start) & (df["trade_date"] <= end)].reset_index(drop=True)
        if self.inject_defects and not df.empty:
            df = self._inject_defects(df)
        return self._finalize(df)

    @staticmethod
    def _inject_defects(df: pd.DataFrame) -> pd.DataFrame:
        """Corrupt ~0.4% of rows in ways real vendor feeds do."""
        df = df.astype({"volume": "float64"})
        picks = [
            _seed("defect", r.symbol, r.trade_date) % 10_000 < DEFECT_RATE * 10_000
            for r in df[["symbol", "trade_date"]].itertuples()
        ]
        dupes = []
        for i in np.flatnonzero(picks):
            kind = _seed("kind", df.at[i, "symbol"], df.at[i, "trade_date"]) % 5
            if kind == 0:
                df.at[i, "close"] = np.nan  # missing value
            elif kind == 1:
                df.at[i, "volume"] = -df.at[i, "volume"]  # sign error
            elif kind == 2:
                df.at[i, "high"], df.at[i, "low"] = df.at[i, "low"], df.at[i, "high"]  # swapped
            elif kind == 3:
                df.at[i, "close"] = df.at[i, "close"] * 100  # fat-finger / unit error
            else:
                dupes.append(df.loc[i])  # duplicate delivery
        if dupes:
            df = pd.concat([df, pd.DataFrame(dupes)], ignore_index=True)
        return df

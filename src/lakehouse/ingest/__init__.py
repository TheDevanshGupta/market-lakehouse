"""Source connectors. Each returns a DataFrame matching RAW_PRICE_COLUMNS.

Only the built-in simulated feed ships with the project, so no third-party data
(and no third-party licence terms) is ever involved. New sources plug in by
subclassing `PriceSource` and registering here.
"""

from __future__ import annotations

import pandas as pd

from lakehouse.ingest.base import RAW_PRICE_COLUMNS, PriceSource


def get_source(name: str, securities: pd.DataFrame) -> PriceSource:
    if name == "synthetic":
        from lakehouse.ingest.synthetic import SyntheticSource

        return SyntheticSource(securities)
    raise ValueError(f"Unknown source '{name}'. Available: 'synthetic'.")


__all__ = ["RAW_PRICE_COLUMNS", "PriceSource", "get_source"]

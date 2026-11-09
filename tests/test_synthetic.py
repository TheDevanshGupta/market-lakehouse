from datetime import date

from lakehouse.ingest import RAW_PRICE_COLUMNS
from lakehouse.ingest.synthetic import SyntheticSource


def test_schema_and_trading_days(securities):
    df = SyntheticSource(securities, inject_defects=False).fetch(
        date(2024, 3, 4), date(2024, 3, 10)
    )
    assert list(df.columns) == RAW_PRICE_COLUMNS
    # Mon 4th - Fri 8th = 5 business days per symbol; weekend excluded.
    assert len(df) == 5 * len(securities)
    assert {d.weekday() for d in df["trade_date"]} <= {0, 1, 2, 3, 4}


def test_deterministic_and_range_independent(securities):
    src = SyntheticSource(securities, inject_defects=False)
    wide = src.fetch(date(2024, 1, 1), date(2024, 2, 29)).set_index(["symbol", "trade_date"])
    narrow = src.fetch(date(2024, 2, 1), date(2024, 2, 29)).set_index(["symbol", "trade_date"])
    # Incremental loads must produce the same bars as a full backfill.
    assert narrow.equals(wide.loc[narrow.index])


def test_clean_data_is_ohlc_consistent(securities):
    df = SyntheticSource(securities, inject_defects=False).fetch(
        date(2024, 1, 1), date(2024, 6, 30)
    )
    assert (df["high"] >= df[["open", "close"]].max(axis=1)).all()
    assert (df["low"] <= df[["open", "close"]].min(axis=1)).all()
    assert (df["volume"] > 0).all()


def test_defects_are_injected(securities):
    clean = SyntheticSource(securities, inject_defects=False).fetch(
        date(2024, 1, 1), date(2024, 12, 31)
    )
    dirty = SyntheticSource(securities).fetch(date(2024, 1, 1), date(2024, 12, 31))
    assert len(dirty) > len(clean)  # duplicates
    assert dirty["close"].isna().any()
    assert (dirty["volume"] < 0).any()

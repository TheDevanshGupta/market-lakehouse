"""End-to-end: backfill, incremental run, idempotent re-run, then query the gold layer."""

from datetime import date

import duckdb
import pytest

from lakehouse import pipeline, storage


@pytest.fixture
def warehouse(settings):
    def query(sql):
        with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
            return con.sql(sql).fetchall()

    return query


def test_backfill_then_incremental(settings, warehouse):
    first = pipeline.run(settings, start=date(2024, 1, 1), end=date(2024, 6, 28))
    assert first.status == "success"
    assert first.rows_quarantined > 0, "synthetic defects should be caught"
    assert storage.Watermark(settings.state_file).get() == date(2024, 6, 28)

    n_symbols = warehouse("select count(*) from marts.dim_security")[0][0]
    n_days = warehouse("select count(distinct trade_date) from marts.fct_daily_prices")[0][0]
    n_rows = warehouse("select count(*) from marts.fct_daily_prices")[0][0]
    # Every quarantined row is a hole in the fact table; nothing else is lost.
    assert n_rows == n_symbols * n_days - first.rows_quarantined

    # Incremental: no start given -> resumes from watermark.
    second = pipeline.run(settings, end=date(2024, 7, 12))
    assert second.start_date == date(2024, 6, 29)
    assert warehouse("select max(trade_date) from marts.fct_daily_prices")[0][0] == date(
        2024, 7, 12
    )

    # Nothing new -> skipped, and the warehouse is unchanged.
    before = warehouse("select count(*) from marts.fct_daily_prices")[0][0]
    third = pipeline.run(settings, end=date(2024, 7, 12))
    assert third.rows_ingested == 0
    assert warehouse("select count(*) from marts.fct_daily_prices")[0][0] == before

    runs = warehouse("select count(distinct run_id) from marts.mart_data_quality")[0][0]
    assert runs == 3


def test_backfill_rerun_is_idempotent(settings, warehouse):
    pipeline.run(settings, start=date(2024, 3, 1), end=date(2024, 3, 29))
    count = warehouse("select count(*) from marts.fct_daily_prices")[0][0]
    pipeline.run(settings, start=date(2024, 3, 1), end=date(2024, 3, 29))
    assert warehouse("select count(*) from marts.fct_daily_prices")[0][0] == count

from datetime import date

import numpy as np
import pandas as pd
import pytest

from lakehouse import quality

KNOWN = {"AAA", "BBB"}


def _bar(symbol="AAA", d=date(2024, 1, 2), o=100.0, h=105.0, lo=95.0, c=102.0, v=1000):
    return {
        "symbol": symbol,
        "trade_date": d,
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "volume": v,
        "source": "test",
    }


def _days(n):
    return list(pd.bdate_range("2024-01-01", periods=n).date)


def _run(rows, context=None):
    return quality.validate(pd.DataFrame(rows), KNOWN, run_id="t", context=context)


def test_clean_batch_passes():
    res = _run([_bar(d=d) for d in _days(5)])
    assert len(res.valid) == 5 and res.quarantined.empty
    assert res.summary["rows_failed"].sum() == 0


@pytest.mark.parametrize(
    "overrides, rule",
    [
        ({"c": np.nan}, "not_null_values"),
        ({"v": -5}, "non_negative_volume"),
        ({"h": 90.0, "lo": 110.0}, "high_low_consistency"),
        ({"o": -1.0}, "positive_prices"),
        ({"symbol": "ZZZ"}, "unknown_symbol"),
        ({"d": date(2024, 1, 6)}, "weekend_trade"),
    ],
)
def test_rule_quarantines_bad_row(overrides, rule):
    rows = [_bar(d=d) for d in _days(5)]
    rows.append(_bar(**{"d": date(2024, 1, 9), **overrides}))
    res = _run(rows)
    assert len(res.quarantined) == 1
    assert rule in res.quarantined.iloc[0]["dq_reasons"]
    assert len(res.valid) == 5


def test_price_spike_detected_against_neighbours():
    days = _days(9)
    rows = [_bar(d=d) for d in days]
    rows[4] = _bar(d=days[4], o=100, h=10_300, lo=95, c=10_200)  # 100x unit error
    res = _run(rows)
    assert res.quarantined["dq_reasons"].tolist() == ["price_spike"]


def test_price_spike_uses_history_for_single_day_batch():
    history = pd.DataFrame([_bar(d=d) for d in _days(6)[:5]])
    spike = _bar(d=_days(6)[5], h=10_300, c=10_200)
    assert len(_run([spike], context=history).quarantined) == 1
    assert len(_run([spike]).quarantined) == 0  # without context there is no baseline


def test_duplicates_dropped_not_quarantined():
    rows = [_bar(), _bar()]
    res = _run(rows)
    assert len(res.valid) == 1 and res.quarantined.empty
    dup = res.summary.set_index("rule_name").loc["duplicate_key"]
    assert dup.rows_failed == 1 and dup.severity == "warn"

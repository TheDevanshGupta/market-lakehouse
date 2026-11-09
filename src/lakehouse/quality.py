"""Declarative data-quality rules with row-level quarantine.

Each rule is a vectorised predicate that flags *bad* rows. Rows failing any rule
are routed to quarantine with every reason attached; the rest flow to silver.
A per-run summary is written to the audit layer and surfaced in dbt + the dashboard.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

Predicate = Callable[[pd.DataFrame], pd.Series]


@dataclass(frozen=True)
class Rule:
    name: str
    description: str
    is_bad: Predicate
    severity: str = "error"  # "error" -> quarantine, "warn" -> keep but count


PRICE_COLS = ["open", "high", "low", "close"]


def _price_spike(df: pd.DataFrame, threshold: float = 0.40) -> pd.Series:
    """Flag a close >40% away from the symbol's local (5-bar centred) median close.

    Exchange circuit limits typically cap daily moves at ~20%, so a 40% gap against the
    neighbouring bars is almost certainly a unit or fat-finger error. `_ref_median`
    is computed in `validate` using recent silver history as context.
    """
    median = df["_ref_median"]
    return ((df["close"] - median).abs() / median) > threshold


def _local_median(batch: pd.DataFrame, context: pd.DataFrame | None) -> pd.Series:
    cols = ["symbol", "trade_date", "close"]
    hist = context[cols].assign(_ctx=True) if context is not None and not context.empty else None
    frame = batch[cols].assign(_ctx=False, _pos=range(len(batch)))
    combined = pd.concat([hist, frame]) if hist is not None else frame
    combined = combined.sort_values(["symbol", "trade_date"], kind="stable")
    combined["_med"] = combined.groupby("symbol")["close"].transform(
        lambda s: s.rolling(5, center=True, min_periods=1).median()
    )
    out = combined[~combined["_ctx"]].set_index("_pos")["_med"]
    return out.reindex(range(len(batch))).to_numpy()


RULES: list[Rule] = [
    Rule(
        "not_null_values",
        "OHLC and volume must be present",
        lambda d: d[[*PRICE_COLS, "volume"]].isna().any(axis=1),
    ),
    Rule(
        "not_null_keys",
        "symbol and trade_date must be present",
        lambda d: d[["symbol", "trade_date"]].isna().any(axis=1),
    ),
    Rule("positive_prices", "OHLC must be > 0", lambda d: (d[PRICE_COLS] <= 0).any(axis=1)),
    Rule("non_negative_volume", "volume must be >= 0", lambda d: d["volume"] < 0),
    Rule(
        "high_low_consistency",
        "high >= max(open, close) and low <= min(open, close)",
        lambda d: (
            (d["high"] < d[["open", "close", "low"]].max(axis=1))
            | (d["low"] > d[["open", "close", "high"]].min(axis=1))
        ),
    ),
    Rule("price_spike", "close within 40% of the symbol's batch median", _price_spike),
    Rule(
        "weekend_trade",
        "no trades on Saturday/Sunday",
        lambda d: pd.to_datetime(d["trade_date"]).dt.dayofweek >= 5,
    ),
    Rule("unknown_symbol", "symbol must exist in the security master", lambda d: d["_unknown"]),
    Rule(
        "duplicate_key",
        "one row per (symbol, trade_date); later copies flagged",
        lambda d: d.duplicated(subset=["symbol", "trade_date"], keep="first"),
        severity="warn",
    ),
]


@dataclass
class QualityResult:
    valid: pd.DataFrame
    quarantined: pd.DataFrame
    summary: pd.DataFrame


def validate(
    batch: pd.DataFrame,
    known_symbols: set[str],
    run_id: str,
    context: pd.DataFrame | None = None,
) -> QualityResult:
    """Validate a batch. `context` = recent trusted rows used for statistical checks."""
    df = batch.reset_index(drop=True).copy()
    df["_unknown"] = ~df["symbol"].isin(known_symbols)
    df["_ref_median"] = _local_median(df, context) if not df.empty else []

    flags = pd.DataFrame({r.name: r.is_bad(df).fillna(False).astype(bool) for r in RULES})
    error_rules = [r.name for r in RULES if r.severity == "error"]
    is_bad = flags[error_rules].any(axis=1)
    # Duplicates are dropped from silver (not quarantined) because they carry no new information.
    is_dupe = flags["duplicate_key"] & ~is_bad

    reasons = flags[error_rules].apply(lambda row: ",".join(row.index[row]), axis=1)
    df = df.drop(columns=["_unknown", "_ref_median"])

    quarantined = df[is_bad].assign(dq_reasons=reasons[is_bad], run_id=run_id)
    valid = df[~is_bad & ~is_dupe].astype({"volume": "int64"})

    summary = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "rule_name": r.name,
                "description": r.description,
                "severity": r.severity,
                "rows_checked": len(df),
                "rows_failed": int(flags[r.name].sum()),
            }
            for r in RULES
        ]
    )
    return QualityResult(valid=valid, quarantined=quarantined, summary=summary)

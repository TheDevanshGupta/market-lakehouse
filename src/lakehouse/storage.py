"""Partitioned Parquet IO and the incremental-load watermark."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd


def write_partitioned(df: pd.DataFrame, root: Path, run_id: str) -> int:
    """Write one file per trade_date partition. Re-running a date overwrites that run's file only."""
    if df.empty:
        return 0
    root.mkdir(parents=True, exist_ok=True)
    for trade_date, part in df.groupby("trade_date"):
        part_dir = root / f"trade_date={trade_date}"
        part_dir.mkdir(exist_ok=True)
        part.drop(columns="trade_date").to_parquet(part_dir / f"{run_id}.parquet", index=False)
    return df["trade_date"].nunique()


def replace_partitions(df: pd.DataFrame, root: Path, run_id: str) -> int:
    """Idempotent upsert: clear each touched partition, then write it fresh."""
    for trade_date in df["trade_date"].unique():
        part_dir = root / f"trade_date={trade_date}"
        if part_dir.exists():
            for f in part_dir.glob("*.parquet"):
                f.unlink()
    return write_partitioned(df, root, run_id)


def read_partitions(root: Path, run_id: str) -> pd.DataFrame:
    files = sorted(root.glob(f"trade_date=*/{run_id}.parquet"))
    frames = []
    for f in files:
        part = pd.read_parquet(f)
        part["trade_date"] = date.fromisoformat(f.parent.name.split("=", 1)[1])
        frames.append(part)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


class Watermark:
    """Tracks the last successfully processed trade date so runs are incremental."""

    def __init__(self, path: Path):
        self.path = path

    def get(self) -> date | None:
        if not self.path.exists():
            return None
        return date.fromisoformat(json.loads(self.path.read_text())["last_trade_date"])

    def set(self, value: date, run_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(
                {
                    "last_trade_date": value.isoformat(),
                    "run_id": run_id,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                indent=2,
            )
        )

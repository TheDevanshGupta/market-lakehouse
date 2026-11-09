"""Central configuration. Every path can be overridden with an environment variable."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    dbt_dir: Path
    warehouse_path: Path
    source: str  # only "synthetic" ships; see ingest/__init__.py
    default_start: str

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze" / "prices"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver" / "prices"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine" / "prices"

    @property
    def audit_dir(self) -> Path:
        return self.data_dir / "audit"

    @property
    def state_file(self) -> Path:
        return self.data_dir / "state" / "watermark.json"

    @property
    def securities_file(self) -> Path:
        return self.dbt_dir / "seeds" / "securities.csv"


def get_settings() -> Settings:
    data_dir = Path(os.getenv("LAKEHOUSE_DATA_DIR", PROJECT_ROOT / "data")).resolve()
    return Settings(
        data_dir=data_dir,
        dbt_dir=PROJECT_ROOT / "dbt",
        warehouse_path=Path(
            os.getenv("LAKEHOUSE_WAREHOUSE", data_dir / "warehouse.duckdb")
        ).resolve(),
        source=os.getenv("LAKEHOUSE_SOURCE", "synthetic"),
        default_start=os.getenv("LAKEHOUSE_START_DATE", "2024-01-01"),
    )


def load_securities(settings: Settings) -> pd.DataFrame:
    return pd.read_csv(settings.securities_file)

import pandas as pd
import pytest

from lakehouse.config import Settings, get_settings


@pytest.fixture
def settings(tmp_path) -> Settings:
    base = get_settings()
    return Settings(
        data_dir=tmp_path / "data",
        dbt_dir=base.dbt_dir,
        warehouse_path=tmp_path / "data" / "warehouse.duckdb",
        source="synthetic",
        default_start="2024-01-01",
    )


@pytest.fixture
def securities(settings) -> pd.DataFrame:
    return pd.read_csv(settings.securities_file)

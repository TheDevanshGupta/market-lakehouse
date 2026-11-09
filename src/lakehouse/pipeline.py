"""Pipeline stages: extract -> bronze -> validate -> silver/quarantine -> dbt (gold).

Each stage is a plain function so it can be driven by the CLI, tests, or an
orchestrator (see orchestration/definitions.py) without changes.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from lakehouse import quality, storage
from lakehouse.config import Settings, load_securities
from lakehouse.ingest import get_source

log = logging.getLogger("lakehouse")

CONTEXT_DAYS = 10


@dataclass
class RunReport:
    run_id: str
    source: str
    start_date: date | None
    end_date: date | None
    rows_ingested: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    rows_duplicate: int = 0
    partitions_written: int = 0
    status: str = "skipped"
    started_at: str = ""
    finished_at: str = ""


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")


def plan_range(settings: Settings, start: date | None, end: date | None) -> tuple[date, date]:
    """Explicit dates win (backfill); otherwise resume from the watermark."""
    end = end or date.today()
    if start is None:
        last = storage.Watermark(settings.state_file).get()
        start = last + timedelta(days=1) if last else date.fromisoformat(settings.default_start)
    return start, end


def extract(settings: Settings, start: date, end: date, run_id: str) -> int:
    securities = load_securities(settings)
    source = get_source(settings.source, securities)
    log.info("Extracting %s -> %s from '%s'", start, end, source.name)
    raw = source.fetch(start, end)
    raw["ingested_at"] = pd.Timestamp.now(tz="UTC")
    raw["run_id"] = run_id
    storage.write_partitioned(raw, settings.bronze_dir, run_id)
    log.info("Bronze: %d rows landed", len(raw))
    return len(raw)


def _silver_context(settings: Settings, before: date) -> pd.DataFrame:
    frames = []
    for d in pd.date_range(before - timedelta(days=CONTEXT_DAYS), before - timedelta(days=1)):
        part_dir = settings.silver_dir / f"trade_date={d.date()}"
        for f in part_dir.glob("*.parquet"):
            frames.append(pd.read_parquet(f).assign(trade_date=d.date()))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def validate_and_publish(settings: Settings, start: date, run_id: str) -> quality.QualityResult:
    bronze = storage.read_partitions(settings.bronze_dir, run_id)
    known = set(load_securities(settings)["symbol"])
    result = quality.validate(
        bronze.drop(columns=["ingested_at", "run_id"]),
        known_symbols=known,
        run_id=run_id,
        context=_silver_context(settings, start),
    )
    valid = result.valid.assign(
        ingested_at=bronze.loc[result.valid.index, "ingested_at"].values,
        run_id=run_id,
    )
    storage.replace_partitions(valid, settings.silver_dir, run_id)
    storage.write_partitioned(result.quarantined, settings.quarantine_dir, run_id)

    audit = settings.audit_dir / "dq_rule_results"
    audit.mkdir(parents=True, exist_ok=True)
    result.summary.to_parquet(audit / f"{run_id}.parquet", index=False)
    log.info(
        "Silver: %d valid | %d quarantined | %d duplicates dropped",
        len(result.valid),
        len(result.quarantined),
        len(bronze) - len(result.valid) - len(result.quarantined),
    )
    return result


def transform(settings: Settings, select: str | None = None, full_refresh: bool = False) -> None:
    """Run `dbt build` (seed + run + test) in a child process.

    A subprocess (rather than dbtRunner in-process) guarantees dbt's DuckDB
    connection is closed afterwards, so readers can open the warehouse.
    """
    env = {
        **os.environ,
        "LAKEHOUSE_DATA_DIR": settings.data_dir.as_posix(),
        "LAKEHOUSE_WAREHOUSE": settings.warehouse_path.as_posix(),
    }
    args = [
        sys.executable,
        "-c",
        "import sys; from dbt.cli.main import cli; sys.exit(cli())",
        "build",
        "--project-dir",
        str(settings.dbt_dir),
        "--profiles-dir",
        str(settings.dbt_dir),
    ]
    if select:
        args += ["--select", select]
    if full_refresh:
        args.append("--full-refresh")
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    if subprocess.call(args, env=env, cwd=settings.dbt_dir) != 0:
        raise RuntimeError("dbt build failed - see output above / dbt/logs/dbt.log")


def write_run_report(settings: Settings, report: RunReport) -> None:
    out = settings.audit_dir / "pipeline_runs"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([asdict(report)]).astype(
        {"start_date": "string", "end_date": "string"}
    ).to_parquet(out / f"{report.run_id}.parquet", index=False)


def run(
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    skip_transform: bool = False,
) -> RunReport:
    run_id = new_run_id()
    start, end = plan_range(settings, start, end)
    report = RunReport(
        run_id=run_id,
        source=settings.source,
        start_date=start,
        end_date=end,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    if start > end:
        log.info("Up to date (watermark >= %s); nothing to ingest.", end)
    else:
        try:
            report.rows_ingested = extract(settings, start, end, run_id)
            if report.rows_ingested:
                result = validate_and_publish(settings, start, run_id)
                report.rows_valid = len(result.valid)
                report.rows_quarantined = len(result.quarantined)
                report.rows_duplicate = (
                    report.rows_ingested - report.rows_valid - report.rows_quarantined
                )
                report.partitions_written = result.valid["trade_date"].nunique()
                storage.Watermark(settings.state_file).set(max(result.valid["trade_date"]), run_id)
            report.status = "success"
        except Exception:
            report.status = "failed"
            report.finished_at = datetime.now(timezone.utc).isoformat()
            write_run_report(settings, report)
            raise

    report.finished_at = datetime.now(timezone.utc).isoformat()
    write_run_report(settings, report)
    if not skip_transform:
        transform(settings)
    return report

"""Command line entry point: `lakehouse run`, `lakehouse transform`, `lakehouse dashboard`."""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date

from lakehouse import pipeline
from lakehouse.config import PROJECT_ROOT, get_settings


def _date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Incremental extract -> validate -> dbt build")
    run_p.add_argument("--start", type=_date, help="Backfill start (default: watermark + 1)")
    run_p.add_argument("--end", type=_date, help="Backfill end (default: today)")
    run_p.add_argument("--skip-transform", action="store_true", help="Stop after silver")

    tr_p = sub.add_parser("transform", help="Run dbt build only")
    tr_p.add_argument("--select", help="dbt selector, e.g. 'marts'")
    tr_p.add_argument("--full-refresh", action="store_true")

    sub.add_parser("dashboard", help="Launch the Streamlit dashboard")

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = get_settings()

    if args.command == "run":
        report = pipeline.run(settings, args.start, args.end, args.skip_transform)
        print(
            f"\nRun {report.run_id}: {report.status} | {report.start_date} -> {report.end_date} | "
            f"ingested={report.rows_ingested} valid={report.rows_valid} "
            f"quarantined={report.rows_quarantined} duplicates={report.rows_duplicate}"
        )
    elif args.command == "transform":
        pipeline.transform(settings, args.select, args.full_refresh)
    elif args.command == "dashboard":
        app = PROJECT_ROOT / "dashboard" / "app.py"
        return subprocess.call([sys.executable, "-m", "streamlit", "run", str(app)])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

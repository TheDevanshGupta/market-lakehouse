"""Dagster orchestration (optional): `dagster dev -f orchestration/definitions.py`.

Wraps the same pipeline functions the CLI uses as software-defined assets, with
run metadata surfaced in the Dagster UI and a weekday evening schedule (after Indian market hours).
"""

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from lakehouse import pipeline
from lakehouse.config import get_settings


@asset(group_name="ingestion", description="Extract -> bronze -> DQ rules -> silver/quarantine.")
def silver_prices(context: AssetExecutionContext) -> MaterializeResult:
    report = pipeline.run(get_settings(), skip_transform=True)
    context.log.info("Run %s %s", report.run_id, report.status)
    return MaterializeResult(
        metadata={
            "run_id": report.run_id,
            "range": f"{report.start_date} -> {report.end_date}",
            "rows_ingested": report.rows_ingested,
            "rows_valid": report.rows_valid,
            "rows_quarantined": report.rows_quarantined,
            "quarantine_rate": MetadataValue.float(
                report.rows_quarantined / report.rows_ingested if report.rows_ingested else 0.0
            ),
        }
    )


@asset(
    group_name="transformation",
    deps=[silver_prices],
    description="dbt build: staging -> intermediate -> marts, with all data tests.",
)
def gold_marts() -> MaterializeResult:
    settings = get_settings()
    pipeline.transform(settings)
    return MaterializeResult(metadata={"warehouse": str(settings.warehouse_path)})


daily_job = define_asset_job("daily_market_refresh", selection="*")

defs = Definitions(
    assets=[silver_prices, gold_marts],
    jobs=[daily_job],
    schedules=[
        ScheduleDefinition(
            job=daily_job,
            cron_schedule="0 19 * * 1-5",
            execution_timezone="Asia/Kolkata",
        )
    ],
)

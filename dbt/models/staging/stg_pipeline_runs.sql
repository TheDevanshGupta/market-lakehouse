select
    run_id,
    source                              as data_source,
    cast(start_date as date)            as start_date,
    cast(end_date as date)              as end_date,
    rows_ingested,
    rows_valid,
    rows_quarantined,
    rows_duplicate,
    partitions_written,
    status,
    cast(started_at as timestamptz)     as started_at,
    cast(nullif(finished_at, '') as timestamptz) as finished_at
from {{ source('audit', 'pipeline_runs') }}

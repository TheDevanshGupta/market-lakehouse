-- One row per (symbol, trade_date). If a date was re-delivered by a later run,
-- keep the most recently ingested version.
with source as (
    select * from {{ source('silver', 'prices') }}
),

typed as (
    select
        upper(trim(symbol))             as symbol,
        cast(trade_date as date)        as trade_date,
        cast(open as decimal(18, 2))    as open_price,
        cast(high as decimal(18, 2))    as high_price,
        cast(low as decimal(18, 2))     as low_price,
        cast(close as decimal(18, 2))   as close_price,
        cast(volume as bigint)          as volume,
        source                          as data_source,
        cast(ingested_at as timestamp)  as ingested_at,
        run_id
    from source
)

select *
from typed
qualify row_number() over (
    partition by symbol, trade_date
    order by ingested_at desc
) = 1

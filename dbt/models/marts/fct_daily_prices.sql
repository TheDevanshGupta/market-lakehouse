{{
    config(
        materialized='incremental',
        unique_key=['symbol', 'trade_date'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per security per trading day. Incremental: only the last
-- `incremental_lookback_days` are recomputed, so daily runs stay O(new data).
select
    p.symbol,
    p.trade_date,
    cast(strftime(p.trade_date, '%Y%m%d') as integer) as date_key,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.prev_close_price,
    p.volume,
    p.turnover,
    p.price_change,
    p.daily_return,
    p.log_return,
    p.intraday_range_pct,
    p.gap_pct,
    p.data_source,
    p.ingested_at,
    current_timestamp as dbt_loaded_at
from {{ ref('int_prices_enriched') }} as p

{% if is_incremental() %}
where p.trade_date >= (
    select coalesce(max(trade_date), date '1900-01-01') - interval {{ var('incremental_lookback_days') }} day
    from {{ this }}
)
{% endif %}

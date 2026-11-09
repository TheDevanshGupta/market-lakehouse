-- Day-over-day metrics that need the previous bar for the same symbol.
with prices as (
    select * from {{ ref('stg_prices') }}
),

with_prev as (
    select
        *,
        lag(close_price) over w as prev_close_price
    from prices
    window w as (partition by symbol order by trade_date)
)

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    prev_close_price,
    volume,
    close_price * volume                                         as turnover,
    close_price - prev_close_price                               as price_change,
    (close_price / nullif(prev_close_price, 0)) - 1              as daily_return,
    ln(close_price / nullif(prev_close_price, 0))                as log_return,
    (high_price - low_price) / nullif(prev_close_price, 0)       as intraday_range_pct,
    (open_price / nullif(prev_close_price, 0)) - 1               as gap_pct,
    data_source,
    ingested_at
from with_prev

with first_last as (
    select symbol, min(trade_date) as first_trade_date, max(trade_date) as last_trade_date
    from {{ ref('stg_prices') }}
    group by symbol
)

select
    s.symbol,
    s.company_name,
    s.sector,
    s.industry,
    f.first_trade_date,
    f.last_trade_date,
    f.symbol is not null as has_price_history
from {{ ref('stg_securities') }} as s
left join first_last as f using (symbol)

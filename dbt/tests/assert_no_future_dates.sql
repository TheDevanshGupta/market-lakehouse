select symbol, trade_date
from {{ ref('fct_daily_prices') }}
where trade_date > current_date

-- Singular test: every bar in the fact table must have a coherent OHLC shape.
select symbol, trade_date, open_price, high_price, low_price, close_price
from {{ ref('fct_daily_prices') }}
where high_price < greatest(open_price, close_price, low_price)
   or low_price > least(open_price, close_price, high_price)
   or close_price <= 0

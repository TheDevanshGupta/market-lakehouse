-- Sector breadth and equal-weighted performance per trading day.
select
    f.trade_date,
    s.sector,
    count(*)                                          as constituents,
    avg(f.daily_return)                               as equal_weight_return,
    sum(f.turnover)                                   as total_turnover,
    count(*) filter (where f.daily_return > 0)        as advancers,
    count(*) filter (where f.daily_return < 0)        as decliners,
    arg_max(f.symbol, f.daily_return)                 as top_gainer,
    max(f.daily_return)                               as top_gainer_return,
    arg_min(f.symbol, f.daily_return)                 as top_loser,
    min(f.daily_return)                               as top_loser_return
from {{ ref('fct_daily_prices') }} as f
join {{ ref('dim_security') }} as s using (symbol)
where f.daily_return is not null
group by 1, 2

-- Calendar spanning all loaded data; is_trading_day marks dates with any bar.
with bounds as (
    select min(trade_date) as d0, max(trade_date) as d1 from {{ ref('stg_prices') }}
),

calendar as (
    select cast(unnest(generate_series(d0, d1, interval 1 day)) as date) as calendar_date
    from bounds
),

trading as (
    select distinct trade_date from {{ ref('stg_prices') }}
)

select
    cast(strftime(c.calendar_date, '%Y%m%d') as integer) as date_key,
    c.calendar_date,
    year(c.calendar_date)                                 as year,
    quarter(c.calendar_date)                              as quarter,
    month(c.calendar_date)                                as month,
    monthname(c.calendar_date)                            as month_name,
    weekofyear(c.calendar_date)                           as week_of_year,
    dayname(c.calendar_date)                              as day_name,
    isodow(c.calendar_date) >= 6                          as is_weekend,
    t.trade_date is not null                              as is_trading_day,
    -- Indian fiscal year runs April-March (FY25 = Apr 2024 - Mar 2025).
    case when month(c.calendar_date) >= 4
         then year(c.calendar_date) + 1 else year(c.calendar_date) end as fiscal_year
from calendar as c
left join trading as t on t.trade_date = c.calendar_date

-- Classic technical indicators per security per day, computed with window functions.
with base as (
    select
        symbol,
        trade_date,
        close_price,
        volume,
        daily_return,
        greatest(close_price - prev_close_price, 0) as gain,
        greatest(prev_close_price - close_price, 0) as loss,
        row_number() over (partition by symbol order by trade_date) as bar_number
    from {{ ref('fct_daily_prices') }}
),

windows as (
    select
        *,
        avg(close_price) over (partition by symbol order by trade_date rows 19 preceding)  as sma_20,
        avg(close_price) over (partition by symbol order by trade_date rows 49 preceding)  as sma_50,
        avg(close_price) over (partition by symbol order by trade_date rows 199 preceding) as sma_200,
        stddev_samp(daily_return) over (partition by symbol order by trade_date rows 19 preceding)
            * sqrt(252)                                                                    as volatility_20d_annualised,
        avg(gain) over (partition by symbol order by trade_date rows 13 preceding)         as avg_gain_14,
        avg(loss) over (partition by symbol order by trade_date rows 13 preceding)         as avg_loss_14,
        avg(volume) over (partition by symbol order by trade_date rows 19 preceding)       as avg_volume_20,
        max(close_price) over (partition by symbol order by trade_date rows 251 preceding) as high_52w,
        min(close_price) over (partition by symbol order by trade_date rows 251 preceding) as low_52w
    from base
),

indicators as (
    select
        symbol,
        trade_date,
        close_price,
        case when bar_number >= 20 then sma_20 end  as sma_20,
        case when bar_number >= 50 then sma_50 end  as sma_50,
        case when bar_number >= 200 then sma_200 end as sma_200,
        case when bar_number >= 20 then volatility_20d_annualised end as volatility_20d_annualised,
        case
            when bar_number < 15 then null
            when avg_loss_14 = 0 then 100
            else 100 - 100 / (1 + avg_gain_14 / avg_loss_14)
        end                                          as rsi_14,
        volume / nullif(avg_volume_20, 0)            as relative_volume,
        high_52w,
        low_52w,
        (close_price / high_52w) - 1                 as pct_from_52w_high
    from windows
)

select
    *,
    case
        when sma_50 is null or sma_200 is null then 'insufficient_history'
        when sma_50 > sma_200 then 'bullish'
        else 'bearish'
    end as trend_regime,
    case
        when rsi_14 >= 70 then 'overbought'
        when rsi_14 <= 30 then 'oversold'
        when rsi_14 is not null then 'neutral'
    end as rsi_signal
from indicators

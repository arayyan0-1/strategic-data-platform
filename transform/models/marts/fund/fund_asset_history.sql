{# The 12-month trend of ten assets at the end of each month, with the excess return that follows.
   The assets are the developed-market equity region, the Treasury, TIPS and Baa corporate bonds,
   gold, the broad commodity index and bitcoin. trend is the return of months t-11 to t less the
   T-bill return of the same months, as in fund_signal_history, and a month has one only when
   all 12 months have a return. state is +1 when trend is above 0 and -1 otherwise, so a trend of
   exactly 0 is down and no state is 0. fwd_excess is the return less the T-bill return of month
   t+1, null for the last month of an asset. trend_skip and state_skip are the same over months
   t-12 to t-1: a series built from monthly averages (gold, Baa, the commodity index) has returns
   that correlate with the next month's by construction, which a trend that holds month t picks
   up. #}

with assets (index_key) as (

    values ('region:developed'), ('bond:ust_2y'), ('bond:ust_5y'), ('bond:ust_10y'),
           ('bond:ust_30y'), ('bond:tips_10y'), ('bond:corp_baa'), ('gold'), ('commodity:all'),
           ('crypto:bitcoin')

), returns as (

    select
        r.index_key,
        r.month,
        year(r.month) * 12 + month(r.month)              as month_number,
        r.ret,
        r.rf
    from {{ ref('int_index_returns') }} r
    join assets using (index_key)

), windowed as (

    select
        *,
        product(1 + ret) over trend_window               as trend_growth,
        product(1 + rf) over trend_window                as trend_cash_growth,
        count(*) over trend_window                       as trend_months,
        product(1 + ret) over skip_window                as skip_growth,
        product(1 + rf) over skip_window                 as skip_cash_growth,
        count(*) over skip_window                        as skip_months,
        max(ret) over next_window                        as fwd_ret,
        max(rf) over next_window                         as fwd_rf
    from returns
    window
        trend_window as (partition by index_key order by month_number
                         range between 11 preceding and current row),
        skip_window  as (partition by index_key order by month_number
                         range between 12 preceding and 1 preceding),
        next_window  as (partition by index_key order by month_number
                         range between 1 following and 1 following)

)

select
    index_key,
    month,
    trend_growth - trend_cash_growth                     as trend,
    fwd_ret - fwd_rf                                     as fwd_excess,
    case when trend_growth - trend_cash_growth > 0 then 1 else -1 end   as state,
    case when skip_months = 12 then skip_growth - skip_cash_growth end   as trend_skip,
    case when skip_months = 12
         then case when skip_growth - skip_cash_growth > 0 then 1 else -1 end end
                                                         as state_skip
from windowed
where trend_months = 12
order by index_key, month

{# The signals of the index history at the end of each month, with the return that follows.
   mom is the return of months t-11 to t-1; val is minus the return of months t-59 to t-12 (the
   five-year reversal); trend is the return of months t-11 to t less the T-bill return of the same
   months. A signal is null unless every month of its window has a return. fwd_ret and
   fwd_excess are the return and the excess return of month t+1, null for the last month of an
   index. us:market has its own universe: it has no cross-section and serves the timing study. #}

with indexes as (

    select
        index_key,
        case when index_key like 'region:%'  then 'regions'
             when index_key like 'country:%' then 'countries'
             when index_key in ('us:small', 'us:big', 'us:value', 'us:growth', 'us:momentum',
                                'us:quality', 'us:low_vol')   then 'us_segments'
             when index_key = 'us:market'    then 'us_market'
        end                                              as universe,
        month,
        year(month) * 12 + month(month)                  as month_number,
        ret,
        rf
    from {{ ref('int_index_returns') }}

), windowed as (

    select
        *,
        product(1 + ret) over mom_window                 as mom_growth,
        count(*) over mom_window                         as mom_months,
        product(1 + ret) over val_window                 as val_growth,
        count(*) over val_window                         as val_months,
        product(1 + ret) over trend_window               as trend_growth,
        product(1 + rf) over trend_window                as trend_cash_growth,
        count(*) over trend_window                       as trend_months,
        max(ret) over next_window                        as fwd_ret,
        max(rf) over next_window                         as fwd_rf
    from indexes
    where universe is not null
    window
        mom_window   as (partition by index_key order by month_number
                         range between 11 preceding and 1 preceding),
        val_window   as (partition by index_key order by month_number
                         range between 59 preceding and 12 preceding),
        trend_window as (partition by index_key order by month_number
                         range between 11 preceding and current row),
        next_window  as (partition by index_key order by month_number
                         range between 1 following and 1 following)

)

select
    index_key,
    universe,
    month,
    mom_growth - 1                                       as mom,
    case when val_months = 48 then 1 - val_growth end    as val,
    trend_growth - trend_cash_growth                     as trend,
    fwd_ret,
    fwd_ret - fwd_rf                                     as fwd_excess
from windowed
where mom_months = 11 and trend_months = 12
order by index_key, month

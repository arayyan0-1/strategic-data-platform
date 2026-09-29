{#
  Calendar effects in the US market and the Fama-French factors, from the daily French
  library since July 1963, and again since 2000. The market is mkt_rf plus rf. Three
  calendars:

  - month: the month of the year.
  - weekday: the day of the week.
  - turn: the trading day around the turn of the month, the last three and the first
    three, and the middle of the month.

  mean is the mean daily return in the bucket, excess its difference from the mean of
  all days, and t the t-statistic of the excess (its standard error from the bucket).
  Five years of our own data is too short for a calendar effect, so this reads the
  library, which lags about two months.
#}

with days as (

    select
        date,
        mkt_rf + rf as market, smb, hml, rmw, cma, mom,
        row_number() over (partition by year(date), month(date) order by date)      as td,
        row_number() over (partition by year(date), month(date) order by date desc) as td_back
    from {{ ref('stg_french__factors') }}
    where date >= date '1963-07-01'

), long as (

    unpivot days on market, smb, hml, rmw, cma, mom into name factor value ret

), periods as (

    select *, 'since 1963' as period from long
    union all
    select *, 'since 2000' from long where date >= date '2000-01-01'

), bucketed as (

    select period, factor, ret, 'month' as calendar, month(date) as bucket_order,
           strftime(date, '%b') as bucket
    from periods
    union all
    select period, factor, ret, 'weekday', isodow(date), strftime(date, '%a')
    from periods
    union all
    select period, factor, ret, 'turn',
           case when td_back <= 3 then 4 - td_back when td <= 3 then 3 + td else 7 end,
           case when td_back <= 3 then printf('day -%d', td_back)
                when td <= 3 then printf('day +%d', td) else 'middle' end
    from periods

), overall as (

    select period, factor, avg(ret) as mean_all from periods group by all

)

select
    b.period,
    b.calendar,
    b.factor,
    b.bucket_order,
    b.bucket,
    count(*)                                                        as n_days,
    avg(b.ret)                                                      as mean,
    avg(b.ret) - any_value(o.mean_all)                              as excess,
    (avg(b.ret) - any_value(o.mean_all)) / nullif(stddev_samp(b.ret) / sqrt(count(*)), 0) as t,
    avg((b.ret > 0)::int)                                           as share_up
from bucketed b
join overall o using (period, factor)
group by b.period, b.calendar, b.factor, b.bucket_order, b.bucket
order by b.period, b.calendar, b.factor, b.bucket_order

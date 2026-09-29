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
  p is the two-sided p-value of t under the normal distribution.

  A period tests many effects, so a small p is common by chance. n_tests is the count of
  effects of the period (every calendar, bucket and series). significant is true when
  the effect passes the Benjamini-Hochberg procedure at the false discovery rate fdr
  (the var seasonality_fdr) among the tests of its period.
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

), effects as (

    select
        b.period,
        b.calendar,
        b.factor,
        b.bucket_order,
        b.bucket,
        count(*)                                                    as n_days,
        avg(b.ret)                                                  as mean,
        avg(b.ret) - any_value(o.mean_all)                          as excess,
        (avg(b.ret) - any_value(o.mean_all)) / nullif(stddev_samp(b.ret) / sqrt(count(*)), 0) as t,
        avg((b.ret > 0)::int)                                       as share_up
    from bucketed b
    join overall o using (period, factor)
    group by b.period, b.calendar, b.factor, b.bucket_order, b.bucket

), scaled as (

    -- The tail area of the normal distribution is erfc(z), with z = |t| / sqrt(2).
    -- k is the argument of the polynomial in the approximation of erfc.
    select *, abs(t) / sqrt(2) as z, 1 / (1 + 0.5 * abs(t) / sqrt(2)) as k
    from effects

), tested as (

    -- The rational approximation of erfc of Numerical Recipes (erfcc). Its relative
    -- error is below 1.2e-7 for every z.
    select
        *,
        least(1.0, k * exp(-z * z - 1.26551223 + k * (1.00002368 + k * (0.37409196
            + k * (0.09678418 + k * (-0.18628806 + k * (0.27886807 + k * (-1.13520398
            + k * (1.48851587 + k * (-0.82215223 + k * 0.17087277))))))))))   as p
    from scaled

), ranked as (

    select
        *,
        count(p) over (partition by period)                         as n_tests,
        row_number() over (partition by period order by p nulls last) as p_rank
    from tested

), limits as (

    -- The largest p that has p <= rank * fdr / n_tests. Every effect up to it passes.
    select period, max(p) as p_limit
    from ranked
    where p <= p_rank * {{ var('seasonality_fdr') }}::double / n_tests
    group by period

)

select
    r.period,
    r.calendar,
    r.factor,
    r.bucket_order,
    r.bucket,
    r.n_days,
    r.mean,
    r.excess,
    r.t,
    r.share_up,
    r.p,
    r.n_tests,
    {{ var('seasonality_fdr') }}::double                            as fdr,
    coalesce(r.p <= l.p_limit, false)                               as significant
from ranked r
left join limits l using (period)
order by r.period, r.calendar, r.factor, r.bucket_order

{# Calendar effects (month, weekday, turn of the month) in the market and the Fama-French
   factors, from the daily French library since 1963 and since 2000. significant:
   Benjamini-Hochberg at seasonality_fdr among the tests of the period. #}

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

    -- The two-sided normal tail area is erfc(z), z = |t| / sqrt(2). k is the polynomial's argument.
    select *, abs(t) / sqrt(2) as z, 1 / (1 + 0.5 * abs(t) / sqrt(2)) as k
    from effects

), tested as (

    -- erfc by the Numerical Recipes approximation (erfcc), relative error below 1.2e-7.
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

    -- The largest p with p <= rank * fdr / n_tests. Every effect up to it passes.
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

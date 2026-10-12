{# The panel of the machine-learning study of index returns: one row per index and month of the
   regions, countries and US segments, with the features known at the end of month t and the
   target. r3, r6 are the returns of the last 3 and 6 months, vol12 the standard deviation of the
   last 12 monthly returns, dd12 the largest fall from a peak of the wealth path over those 12
   months (a number at or below 0; the wealth starts at 1 before the first month). mom, val and
   trend are those of fund_signal_history. target is the return of month t+1 less the mean
   return of that month over the names of the universe with one. A window counts only when all
   its months have a return. #}

with names as (

    select distinct index_key from {{ ref('fund_signal_history') }} where universe <> 'us_market'

), returns as (

    select
        r.index_key,
        r.month,
        year(r.month) * 12 + month(r.month)   as month_number,
        r.ret
    from {{ ref('int_index_returns') }} r
    join names using (index_key)

), windowed as (

    select
        index_key,
        month,
        product(1 + ret) over w3 - 1          as r3,
        count(ret) over w3                    as n3,
        product(1 + ret) over w6 - 1          as r6,
        count(ret) over w6                    as n6,
        stddev_samp(ret) over w12             as vol12,
        count(ret) over w12                   as n12
    from returns
    window
        w3 as (partition by index_key order by month_number
               range between 2 preceding and current row),
        w6 as (partition by index_key order by month_number
               range between 5 preceding and current row),
        w12 as (partition by index_key order by month_number
                range between 11 preceding and current row)

), paths as (

    -- The wealth path of the 12 months that end at each month.
    select
        a.index_key,
        a.month,
        b.month_number,
        product(1 + b.ret) over (partition by a.index_key, a.month order by b.month_number)
                                              as wealth
    from returns a
    join returns b
        on b.index_key = a.index_key and b.month_number between a.month_number - 11
                                                           and a.month_number

), peaks as (

    select
        index_key,
        month,
        wealth,
        greatest(1.0, max(wealth) over (partition by index_key, month order by month_number))
                                              as peak
    from paths

), drawdown as (

    select index_key, month, min(wealth / peak - 1) as dd12
    from peaks
    group by 1, 2

), features as (

    select w.index_key, w.month, w.r3, w.r6, w.vol12, d.dd12
    from windowed w
    join drawdown d using (index_key, month)
    where w.n3 = 3 and w.n6 = 6 and w.n12 = 12

)

select
    h.index_key,
    h.universe,
    h.month,
    f.r3,
    f.r6,
    h.mom,
    h.val,
    f.vol12,
    f.dd12,
    h.trend,
    h.fwd_ret - avg(h.fwd_ret) over (partition by h.universe, h.month)   as target
from {{ ref('fund_signal_history') }} h
join features f using (index_key, month)
where h.universe <> 'us_market'
order by h.index_key, h.month

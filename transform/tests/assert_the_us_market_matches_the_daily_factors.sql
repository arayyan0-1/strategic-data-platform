{# A returned row is a month where us:market differs by over 20 bps from the daily Fama-French
   market compounded over the month, or a result that compares fewer than 120 months. The first
   and last month of the daily file can be partial, so they are left out. The library rounds its
   daily factors to 0.01%, so a compounded month differs a little from the monthly value. #}
with daily as (

    select
        last_day(date)                              as month,
        exp(sum(ln(1 + mkt_rf + rf))) - 1           as ret
    from {{ ref('stg_french__factors') }}
    group by 1

), bounds as (

    select min(month) as first_month, max(month) as last_month
    from daily

), compared as (

    select i.month, i.ret as monthly, d.ret as daily
    from {{ ref('int_index_returns') }} i
    join daily d using (month)
    cross join bounds b
    where i.index_key = 'us:market' and d.month > b.first_month and d.month < b.last_month

)

select month, monthly, daily
from compared
where abs(monthly - daily) > 0.002
union all
select cast(null as date), cast(null as double), cast(null as double)
from compared
having count(*) < 120

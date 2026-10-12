{# The monthly return of each candidate fund, spliced: the fund's own return where it has a full
   month, else the return of the long-history index it maps to (fund_index_map). rf is the US
   T-bill return of the month: the library's, else the monthly compound of core.rates.rf. An
   eligible fund has at least 240 daily returns, so its own prices cover the recent windows and
   only the old months read the index. #}

with candidates as (

    select fund, security_key from {{ ref('fund_candidates') }} where is_candidate

), month_ends as (

    select
        security_key,
        last_day(date_trunc('month', date))   as month,
        arg_max(adj_close_total, date)        as px,
        count(*)                              as n_days
    from {{ ref('security_sessions') }}
    where security_key in (select security_key from candidates)
    group by 1, 2

), etf as (

    -- A full month: the month before has a close too, and the fund traded most of the month.
    select security_key, month, ret
    from (
        select
            security_key,
            month,
            n_days,
            px / lag(px) over w - 1   as ret,
            lag(month) over w         as prev_month
        from month_ends
        window w as (partition by security_key order by month)
    )
    where prev_month = last_day(month - interval 1 month) and n_days >= 15

), mapped as (

    select c.fund, c.security_key, nullif(m.index_key, '') as index_key
    from candidates c
    left join {{ ref('fund_index_map') }} m using (fund)

), months as (

    select x.fund, i.month
    from mapped x
    join {{ ref('int_index_returns') }} i using (index_key)
    union
    select x.fund, e.month
    from mapped x
    join etf e using (security_key)

), cash as (

    select month, ret as rf from {{ ref('int_index_returns') }} where index_key = 'cash:tbill'

), rates as (

    -- Cash after the library's last month: the monthly compound of core.rates.rf.
    select last_day(date_trunc('month', date)) as month, exp(sum(ln(1 + rf))) - 1 as rf
    from {{ ref('rates') }}
    group by 1

)

select
    m.fund,
    m.month,
    coalesce(e.ret, i.ret)   as ret,
    coalesce(c.rf, r.rf)     as rf
from months m
join mapped x using (fund)
left join etf e on e.security_key = x.security_key and e.month = m.month
left join {{ ref('int_index_returns') }} i on i.index_key = x.index_key and i.month = m.month
left join cash c on c.month = m.month
left join rates r on r.month = m.month

{# Monthly signals of each candidate fund at the end of each month, from its spliced return
   series: the fund's own monthly return where it has a full month, else the return of the
   long-history index it maps to (fund_index_map). An eligible fund has at least 240 daily
   returns, so its momentum and trend windows come from its own prices and only the value window
   reads index months, all at least a year old and long published. A window counts only when its
   months are consecutive.
   mom = the 11 months to t-1, val = minus the return from t-59 to t-12, trend = the 12-month
   return less cash, as in fund_signal_history. #}

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

), spliced as (

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

), windows as (

    select
        fund,
        month,
        exp(sum(ln(1 + ret)) over mom_w) - 1                      as mom,
        count(ret) over mom_w                                     as n_mom,
        date_diff('month', min(month) over mom_w, month)          as span_mom,
        exp(sum(ln(1 + ret)) over val_w) - 1                      as past_5y,
        count(ret) over val_w                                     as n_val,
        date_diff('month', min(month) over val_w, month)          as span_val,
        exp(sum(ln(1 + ret)) over trend_w) - exp(sum(ln(1 + rf)) over trend_w)
                                                                  as trend,
        count(ret) over trend_w                                   as n_trend,
        count(rf) over trend_w                                    as n_rf,
        date_diff('month', min(month) over trend_w, month)        as span_trend
    from spliced
    window
        mom_w as (partition by fund order by month rows between 11 preceding and 1 preceding),
        val_w as (partition by fund order by month rows between 59 preceding and 12 preceding),
        trend_w as (partition by fund order by month rows between 11 preceding and current row)

)

select
    fund,
    month,
    case when n_mom = 11 and span_mom = 11 then mom end                        as mom,
    case when n_val = 48 and span_val = 59 then -past_5y end                   as val,
    case when n_trend = 12 and n_rf = 12 and span_trend = 11 then trend end    as trend
from windows

{# Monthly signals of each candidate fund at the end of each month, from its spliced return
   series (int_fund_monthly_returns): the fund's own monthly return where it has a full month,
   else the return of the long-history index it maps to (fund_index_map). A window counts only
   when its months are consecutive.
   mom = the 11 months to t-1, val = minus the return from t-59 to t-12, trend = the 12-month
   return less cash, as in fund_signal_history. #}

with windows as (

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
    from {{ ref('int_fund_monthly_returns') }}
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

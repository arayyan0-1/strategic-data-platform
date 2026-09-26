{#
  One row per factor: compounded returns over the last session, week (5 sessions),
  month (21), quarter (63), the year to date and the last year (252), then the
  volatility and Sharpe ratio of the last year, the Sharpe ratio, t statistic and
  largest drawdown of the whole history. The windows count back from the last session
  of each factor, so a lagged factor (ff) reports up to its own last date. Returns are
  gross of cost.
#}

with r as (

    select
        *,
        row_number() over (partition by family, factor order by date desc) as back,
        year(date) = year(max(date) over (partition by family, factor))    as this_year
    from {{ ref('mart_factor_returns') }}
    where ret is not null

), curve as (

    select
        family, factor, date,
        exp(sum(ln(1 + ret)) over w) as level
    from r
    window w as (partition by family, factor order by date
                 rows between unbounded preceding and current row)

), peaks as (

    select
        family, factor,
        level / max(level) over (partition by family, factor order by date
                                 rows between unbounded preceding and current row) - 1
            as below_peak
    from curve

), drawdown as (

    select family, factor, min(below_peak) as max_drawdown
    from peaks
    group by family, factor

)

select
    r.family,
    r.factor,
    min(r.date)                                                           as first_date,
    max(r.date)                                                           as last_date,
    count(*)                                                              as n_days,
    exp(sum(ln(1 + r.ret)) filter (where r.back <= 1)) - 1                as ret_1d,
    exp(sum(ln(1 + r.ret)) filter (where r.back <= 5)) - 1                as ret_1w,
    exp(sum(ln(1 + r.ret)) filter (where r.back <= 21)) - 1               as ret_1m,
    exp(sum(ln(1 + r.ret)) filter (where r.back <= 63)) - 1               as ret_3m,
    exp(sum(ln(1 + r.ret)) filter (where r.this_year)) - 1                as ret_ytd,
    exp(sum(ln(1 + r.ret)) filter (where r.back <= 252)) - 1              as ret_1y,
    stddev_samp(r.ret) filter (where r.back <= 252) * sqrt(252)           as vol_1y,
    avg(r.ret) filter (where r.back <= 252)
        / nullif(stddev_samp(r.ret) filter (where r.back <= 252), 0) * sqrt(252) as sharpe_1y,
    avg(r.ret) / nullif(stddev_samp(r.ret), 0) * sqrt(252)                as sharpe_all,
    avg(r.ret) / nullif(stddev_samp(r.ret) / sqrt(count(*)), 0)           as t_all,
    any_value(d.max_drawdown)                                             as max_drawdown
from r
join drawdown d using (family, factor)
group by r.family, r.factor

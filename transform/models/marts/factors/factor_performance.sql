{# One row per factor: compounded returns over standard windows counted back from the factor's
   own last session (so a lagged factor reports to its last date), and Sharpe, t and drawdown.
   Gross of cost. Sharpe and t use returns over cash. #}

with r as (

    -- xret: return over cash. Long-shorts and the published market factors are already over cash;
    -- the long portfolios (style market, first eigenportfolio) take off rf.
    select
        f.*,
        f.ret - case when (f.family, f.factor) in (('style', 'market'), ('pca', 'pc1'))
                     then coalesce(c.rf, 0) else 0 end                  as xret,
        row_number() over (partition by f.family, f.factor order by f.date desc) as back,
        year(f.date) = year(max(f.date) over (partition by f.family, f.factor)) as this_year
    from {{ ref('factor_returns') }} f
    left join {{ ref('rates') }} c using (date)
    where f.ret is not null

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
    coalesce(any_value(l.label), r.factor)                                as label,
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
    avg(r.xret) filter (where r.back <= 252)
        / nullif(stddev_samp(r.xret) filter (where r.back <= 252), 0) * sqrt(252) as sharpe_1y,
    avg(r.xret) / nullif(stddev_samp(r.xret), 0) * sqrt(252)              as sharpe_all,
    avg(r.xret) / nullif(stddev_samp(r.xret) / sqrt(count(*)), 0)         as t_all,
    any_value(d.max_drawdown)                                             as max_drawdown
from r
join drawdown d using (family, factor)
left join {{ ref('factor_labels') }} l using (family, factor)
group by r.family, r.factor

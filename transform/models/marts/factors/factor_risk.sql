{#
  The risk state of each factor on its last session, in units of its own history:

  - z_1d is the last return over the volatility of the 63 sessions before it. z_1m is
    the log return of the last 21 sessions over that volatility times the root of 21.
  - vol_63 is the annualized volatility of the last 63 sessions. vol_pctile is the share
    of the history of the factor with a lower or equal vol_63: 0.95 is a high-volatility
    regime for the factor.
  - drawdown is the fall from the peak of the compounded return. dd_pctile is the share
    of the history with a smaller fall: 0.95 is a drawdown deeper than 95% of its days.
  - corr_63 and corr_252 are the correlations with the market factor (style) over 63 and
    252 sessions. A large difference is a change in how the factor trades.
  - crowding_z comes from short interest (factor_crowding), for the styles only.
  - rate_corr_63 is the correlation over 63 sessions with the change of the 10-year
    Treasury yield over the same session. A row of core.rates holds the yield of the
    close before, so the change over session D is the row after D less the row of D. The
    last session has no change yet, so the window ends one session earlier.
#}

with r as (

    select family, factor, date, ret
    from {{ ref('factor_returns') }}
    where ret is not null

), mkt as (

    select date, ret as mret from r where family = 'style' and factor = 'market'

), yields as (

    select date, lead(t_10y) over (order by date) - t_10y as dy_10y
    from {{ ref('rates') }}

), roll as (

    select
        r.*,
        m.mret,
        corr(r.ret, y.dy_10y) over (partition by r.family, r.factor order by r.date
            rows between 62 preceding and current row)                  as rate_corr_63,
        stddev_samp(r.ret) over (partition by r.family, r.factor order by r.date
            rows between 63 preceding and 1 preceding)                   as sd_prev,
        stddev_samp(r.ret) over (partition by r.family, r.factor order by r.date
            rows between 62 preceding and current row) * sqrt(252)      as vol_63,
        exp(sum(ln(1 + r.ret)) over (partition by r.family, r.factor order by r.date
            rows between unbounded preceding and current row))          as level,
        corr(r.ret, m.mret) over (partition by r.family, r.factor order by r.date
            rows between 62 preceding and current row)                  as corr_63,
        corr(r.ret, m.mret) over (partition by r.family, r.factor order by r.date
            rows between 251 preceding and current row)                 as corr_252,
        row_number() over (partition by r.family, r.factor order by r.date desc) as back
    from r
    left join mkt m using (date)
    left join yields y using (date)

), dd as (

    select
        *,
        level / max(level) over (partition by family, factor order by date
            rows between unbounded preceding and current row) - 1       as drawdown
    from roll

), last as (

    select * from dd where back = 1

), month as (

    select family, factor, level as level_21 from dd where back = 22

), history as (

    select
        d.family, d.factor,
        avg((d.vol_63 <= l.vol_63)::int) filter (where d.vol_63 is not null)  as vol_pctile,
        avg((d.drawdown >= l.drawdown)::int)                                 as dd_pctile,
        min(d.drawdown)                                                      as max_drawdown
    from dd d
    join last l using (family, factor)
    group by d.family, d.factor

), crowd as (

    select factor, date as crowding_date, crowding, crowding_z
    from {{ ref('factor_crowding') }}
    qualify row_number() over (partition by factor order by date desc) = 1

)

select
    l.family,
    l.factor,
    coalesce(fl.label, l.factor)                             as label,
    l.date                                                   as last_date,
    l.ret                                                    as ret_1d,
    l.ret / nullif(l.sd_prev, 0)                             as z_1d,
    l.level / m.level_21 - 1                                 as ret_1m,
    ln(l.level / m.level_21) / nullif(l.sd_prev * sqrt(21), 0) as z_1m,
    l.vol_63,
    h.vol_pctile,
    l.drawdown,
    h.dd_pctile,
    h.max_drawdown,
    l.corr_63,
    l.corr_252,
    l.rate_corr_63,
    c.crowding,
    c.crowding_z,
    c.crowding_date
from last l
join history h using (family, factor)
left join month m using (family, factor)
left join crowd c on l.family = 'style' and c.factor = l.factor
left join {{ ref('factor_labels') }} fl on fl.family = l.family and fl.factor = l.factor

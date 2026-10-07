{# Risk state of each factor on its last session, in units of its own history: scores, volatility
   and drawdown percentiles, correlations to the market factor and the 10-year yield change, and
   crowding. #}

with r as (

    select family, factor, date, ret
    from {{ ref('factor_returns') }}
    where ret is not null

), mkt as (

    select date, ret as mret from r where family = 'style' and factor = 'market'

), yields as (

    -- A rates row holds the previous close's yield, so the change over D is the row after D
    -- less the row of D. The last session has no change yet.
    select date, lead(t_10y) over (order by date) - t_10y as dy_10y
    from {{ ref('rates') }}

), roll as (

    select
        r.*,
        m.mret,
        corr(r.ret, y.dy_10y) over (partition by r.family, r.factor order by r.date
            rows between 62 preceding and current row)                  as rate_corr_63,
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
    s1.z                                                     as z_1d,
    l.level / m.level_21 - 1                                 as ret_1m,
    s21.z                                                    as z_1m,
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
left join {{ ref('factor_scores') }} s1
    on s1.family = l.family and s1.factor = l.factor and s1.h = 1 and s1.date = l.date
left join {{ ref('factor_scores') }} s21
    on s21.family = l.family and s21.factor = l.factor and s21.h = 21 and s21.date = l.date
left join crowd c on l.family = 'style' and c.factor = l.factor
left join {{ ref('factor_labels') }} fl on fl.family = l.family and fl.factor = l.factor

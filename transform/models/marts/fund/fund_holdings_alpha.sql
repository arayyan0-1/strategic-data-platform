{# The forecast of each US-equity ETF from the stocks it holds: the holdings-weighted Grinold
   forecast of short_ratio, a year. At the last session of each month, a fund takes its latest
   N-PORT filed before that session whose holdings are at most 200 days old. A stock's forecast
   is shrink * IC * sqrt(12) * omega * z, with z the cross-sectional z-score of -short_ratio over
   the in-universe names of the day (clipped at 3) and omega the specific volatility a year. A
   holding with no forecast counts as 0, and weights are over the holdings that map to a
   security. alpha_mkt is the same forecast of the in-universe US stocks weighted by cap, and
   alpha_neutral = alpha_hold - alpha_mkt is the benchmark-neutral forecast: a total-market fund
   scores about 0, as a fund with no holdings does. #}

with decisions as (

    select max(date) as date
    from {{ ref('security_sessions') }}
    group by date_trunc('month', date)

), stocks as (

    select
        g.security_key,
        g.date,
        least(greatest(
            -(g.short_ratio - avg(g.short_ratio) over d) / nullif(stddev_pop(g.short_ratio) over d, 0),
            -3), 3)                                              as z,
        sqrt(252) * r.spec_vol                                   as omega,
        s.market_cap
    from {{ ref('signals') }} g
    join decisions using (date)
    left join {{ ref('style_residuals') }} r using (security_key, date)
    left join {{ ref('security_sessions') }} s using (security_key, date)
    where g.in_universe and g.short_ratio is not null
    window d as (partition by g.date)

), market as (

    -- Every in-universe stock with a cap; one with no forecast counts as 0, as in a fund.
    select
        u.date,
        sum(u.market_cap * coalesce({{ var('fund_shrink') }} * {{ var('fund_hold_ic') }}
            * sqrt(12) * k.omega * k.z, 0)) / sum(u.market_cap)  as alpha_mkt
    from {{ ref('security_sessions') }} u
    join decisions using (date)
    left join stocks k using (security_key, date)
    where u.in_universe and u.market_cap > 0
    group by 1

), latest as (

    select h.fund, d.date, max(h.report_date) as report_date
    from {{ ref('int_fund_holdings_history') }} h
    join decisions d on h.filed < d.date and d.date - h.report_date <= 200
    group by 1, 2

), holdings as (

    select l.fund, l.date, l.report_date, h.security_key, h.weight
    from latest l
    join {{ ref('int_fund_holdings_history') }} h
        on h.fund = l.fund and h.report_date = l.report_date
    where h.security_key is not null

), funds as (

    select
        h.fund,
        h.date,
        h.report_date,
        sum(h.weight * coalesce(
            {{ var('fund_shrink') }} * {{ var('fund_hold_ic') }} * sqrt(12) * s.omega * s.z, 0))
          / sum(h.weight)                                            as alpha_hold,
        sum(h.weight) filter (where s.z is not null) / sum(h.weight) as weight_scored
    from holdings h
    left join stocks s on s.security_key = h.security_key and s.date = h.date
    group by 1, 2, 3

)

select
    f.fund,
    last_day(f.date)                    as month,
    f.date,
    f.report_date,
    f.alpha_hold,
    m.alpha_mkt,
    f.alpha_hold - m.alpha_mkt          as alpha_neutral,
    f.weight_scored
from funds f
join market m using (date)

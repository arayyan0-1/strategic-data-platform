{#
  The stock-specific moves of the last session: the liquid in-universe names (market
  cap of $2B or more, or trailing dollar volume of $20M or more) with the largest
  residual in units of their own residual volatility (style_residuals). A raw mover list
  mixes the market, the industry and the styles with news. This list keeps the part
  that no factor explains. factor_part is the market, industry and style parts together.
  resid_score is the score of resid_z: the map of the names of a cap of monitor_big_cap
  or more (market_scores), so a score of 3 is as rare as a 3 sigma move of a normal
  distribution for those names. A smaller name uses the same map and passes it more often.
#}

with last_session as (

    select max(date) as d from {{ ref('style_residuals') }}

), today as (

    select
        r.ticker, u.name, u.industry_name, r.weight_cap as cap, u.close, r.ret,
        r.market_part + coalesce(r.industry_part, 0) + r.style_part   as factor_part,
        r.resid, r.resid_z, r.spec_vol,
        u.dollar_volume / nullif(u.adv, 0)                            as rel_volume,
        u.dollar_volume
    from {{ ref('style_residuals') }} r
    join {{ ref('int_universe') }} u on u.ticker = r.ticker and u.date = r.date
    where r.date = (select d from last_session)
      and r.resid_z is not null
      and (r.weight_cap >= 2e9 or u.adv >= 2e7)

), scored as (

    select
        * exclude (q),
        sign(resid_z) * {{ two_sided_z('q') }} as resid_score
    from (
        select *, {{ pool_q('resid_z', ref('market_scores'), 'specific', 1) }} as q
        from today
    )

), lists as (

    select 'residual_up' as list, row_number() over (order by resid_z desc) as rank, *
    from scored
    union all
    select 'residual_down', row_number() over (order by resid_z), *
    from scored

)

select (select d from last_session) as date, *
from lists
where rank <= 15

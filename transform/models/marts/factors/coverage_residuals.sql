{#
  The return of each coverage name split into the parts of the style and industry model
  (style_factor_returns) and a residual, as style_residuals does for the universe. Nothing
  is estimated again: the parts are the exposures of coverage_exposures times the factor
  returns of the session, and the residual is what they leave. A row is dated on the
  session of the return.

  spec_vol is the forecast of the volatility of the residual, and resid_z is the residual in
  units of it. The forecast reads only the sessions before the row:
  - own_vol is the root mean square of the earlier residuals of the name, from the universe
    and from the coverage set together, so a name that leaves the universe keeps its
    history. The weight of a residual halves every spec_vol_half_life rows.
  - spec_vol is own_vol times the vol_regime of the session, which style_residuals
    measures on the universe. There is no size-decile shrinkage, because the deciles
    belong to the universe.

  Both are null until the name has spec_vol_min_obs residuals before the row.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}

with days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('signals') }})

), parts as (

    select
        e.security_key,
        e.ticker,
        f.date,
        e.type_filled,
        e.industry,
        e.market_cap,
        e.fwd_ret_1                                             as ret,
        f.market                                                as market_part,
        case e.industry
            {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
            {% endfor %}end                                     as industry_part,
        {% for s in styles %}e.z_{{ s }} * f.{{ s }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                as style_part
    from {{ ref('coverage_exposures') }} e
    join days d on d.date = e.date
    join {{ ref('style_factor_returns') }} f on f.date = d.next_date
    where e.fwd_ret_1 is not null and f.market is not null

), resid as (

    select
        *,
        ret - market_part - coalesce(industry_part, 0) - style_part  as resid
    from parts

), history as (

    -- The residuals of a name from the universe and from the coverage set. A session
    -- holds a name in one of them only.
    select security_key, date, resid, true as is_coverage
    from resid
    union all
    select security_key, date, resid, false
    from {{ ref('style_residuals') }}
    where security_key in (select security_key from resid)

), ranked as (

    select
        *,
        row_number() over (partition by security_key order by date)  as k
    from history

), own as (

    select
        security_key,
        date,
        is_coverage,
        {{ own_vol() }}                                             as own_vol
    from ranked
    window prev as (partition by security_key order by date
                    rows between unbounded preceding and 1 preceding)

), regime as (

    select date, any_value(vol_regime) as vol_regime
    from {{ ref('style_residuals') }}
    group by date

)

select
    r.security_key,
    r.ticker,
    r.date,
    r.type_filled,
    r.industry,
    r.market_cap,
    r.ret,
    r.market_part,
    r.industry_part,
    r.style_part,
    r.resid,
    o.own_vol,
    g.vol_regime,
    o.own_vol * g.vol_regime                                        as spec_vol,
    r.resid / nullif(o.own_vol * g.vol_regime, 0)                   as resid_z
from resid r
join own o on o.security_key = r.security_key and o.date = r.date and o.is_coverage
join regime g on g.date = r.date
order by r.date, r.security_key

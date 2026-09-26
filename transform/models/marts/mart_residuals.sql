{#
  The return of each in-universe name split into the parts of the style and industry
  model (mart_factor_style) and a residual. The residual is the part that the market,
  the industry and the eight styles do not explain: the stock-specific move, where news
  and alpha live. spec_vol is the volatility of the residual over the 63 sessions
  before, and resid_z is the residual in units of it. A row is dated on the session of
  the return.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}

with days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('mart_signals') }})

), parts as (

    select
        e.security_key,
        e.ticker,
        f.date,
        e.industry,
        e.cap,
        e.w,
        e.r                                                     as ret,
        f.market                                                as market_part,
        case e.industry
            {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
            {% endfor %}end                                     as industry_part,
        {% for s in styles %}e.z_{{ s }} * f.{{ s }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                as style_part
    from {{ ref('mart_style_exposures') }} e
    join days d on d.date = e.date
    join {{ ref('mart_factor_style') }} f on f.date = d.next_date
    where e.r is not null and f.market is not null

), resid as (

    select
        *,
        ret - market_part - coalesce(industry_part, 0) - style_part  as resid
    from parts

)

select
    *,
    stddev_samp(resid) over prev                                     as spec_vol,
    resid / nullif(stddev_samp(resid) over prev, 0)                  as resid_z
from resid
window prev as (partition by security_key order by date
                rows between 63 preceding and 1 preceding)

{#
  The return of each in-universe name split into the parts of the style and industry
  model (style_factor_returns) and a residual. The residual is the part that the market,
  the industry and the eight styles do not explain: the stock-specific move, where news
  and alpha live. A row is dated on the session of the return.

  spec_vol is the forecast of the volatility of the residual, and resid_z is the residual
  in units of it. The forecast reads only the sessions before the row, in three steps:
  - own_vol is the root mean square of the residuals of the name before the row. The
    weight of a residual halves every spec_vol_half_life rows.
  - The shrinkage moves own_vol toward the cap-weighted mean of its size decile on the
    session. The pull grows with the distance from the mean, at the rate
    spec_vol_shrinkage.
  - vol_regime is the square root of the weighted mean, over the sessions before, of the
    cross-section mean of z squared, with z from the forecast of the two steps above. The
    weight of a session halves every spec_vol_regime_half_life sessions. It raises every
    forecast when the residuals of the last sessions were large, and lowers every forecast
    when they were small. spec_vol is the shrunk own_vol times vol_regime.

  All three are null until the name has spec_vol_min_obs residuals before the row, so a
  new listing has no false extreme.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}
{# The running sums below hold the weight decay^(-k), which grows fast with k. A half-life
   below 2 leaves too little room before it overflows, about 2,000 rows. #}
{% if var('spec_vol_half_life') < 2 or var('spec_vol_regime_half_life') < 2 %}
    {{ exceptions.raise_compiler_error(
        'spec_vol_half_life and spec_vol_regime_half_life must be 2 or more.') }}
{% endif %}
{% set regime_decay = 0.5 ** (1.0 / var('spec_vol_regime_half_life')) %}
{% set shrink = var('spec_vol_shrinkage') %}

with days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('signals') }})

), parts as (

    select
        e.security_key,
        e.ticker,
        f.date,
        e.industry,
        e.weight_cap,
        e.weight,
        e.fwd_ret_1                                             as ret,
        f.market                                                as market_part,
        case e.industry
            {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
            {% endfor %}end                                     as industry_part,
        {% for s in styles %}e.z_{{ s }} * f.{{ s }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                as style_part
    from {{ ref('style_exposures') }} e
    join days d on d.date = e.date
    join {{ ref('style_factor_returns') }} f on f.date = d.next_date
    where e.fwd_ret_1 is not null and f.market is not null

), resid as (

    select
        *,
        ret - market_part - coalesce(industry_part, 0) - style_part  as resid,
        row_number() over (partition by security_key order by date)  as k
    from parts

), own as (

    select
        *,
        {{ own_vol() }}                                             as own_vol
    from resid
    window prev as (partition by security_key order by date
                    rows between unbounded preceding and 1 preceding)

), sized as (

    -- The names with no own_vol form their own group, so they do not move the deciles of
    -- the other names.
    select
        *,
        ntile(10) over (partition by date, own_vol is null
                        order by weight_cap desc, security_key)     as size_decile
    from own

), decile_stats as (

    select
        *,
        sum(weight_cap * own_vol) over by_decile
            / sum(case when own_vol is not null then weight_cap end) over by_decile
                                                                    as decile_vol,
        stddev_samp(own_vol) over by_decile                         as decile_spread
    from sized
    window by_decile as (partition by date, size_decile)

), shrunk as (

    select
        *,
        coalesce({{ shrink }} * abs(own_vol - decile_vol)
                 / nullif(coalesce(decile_spread, 0) + {{ shrink }} * abs(own_vol - decile_vol), 0),
                 0)                                                 as shrink_weight
    from decile_stats

), forecast as (

    select
        *,
        shrink_weight * decile_vol + (1 - shrink_weight) * own_vol  as base_vol
    from shrunk

), sessions as (

    select date, row_number() over (order by date) as t
    from (select distinct date from resid)

), z_by_session as (

    select date, avg(power(resid / base_vol, 2)) as mean_z2
    from forecast
    where base_vol > 0
    group by date

), regime as (

    -- Two running sums, as for own_vol, but over sessions. The factor decay^(t - 1) cancels
    -- in the ratio. A session with no z has no weight. When no session before has a z,
    -- the regime is 1.
    select
        s.date,
        coalesce(sqrt(sum(z.mean_z2 * power({{ regime_decay }}, -s.t)) over prev
                      / nullif(sum(case when z.mean_z2 is not null
                                        then power({{ regime_decay }}, -s.t) end) over prev, 0)),
                 1.0)                                               as vol_regime
    from sessions s
    left join z_by_session z on z.date = s.date
    window prev as (order by s.date rows between unbounded preceding and 1 preceding)

)

select
    f.security_key,
    f.ticker,
    f.date,
    f.industry,
    f.weight_cap,
    f.weight,
    f.ret,
    f.market_part,
    f.industry_part,
    f.style_part,
    f.resid,
    f.own_vol,
    r.vol_regime,
    f.base_vol * r.vol_regime                                       as spec_vol,
    f.resid / nullif(f.base_vol * r.vol_regime, 0)                  as resid_z
from forecast f
join regime r on r.date = f.date

-- Recompute the z-scores of a sample of sessions from the characteristics, with the
-- parameters of the universe of the session written out: the winsor bounds (median plus or
-- minus 5 x 1.4826 x MAD), the mean weighted as the style fit weights it, the standard
-- deviation, and the clip. A coverage z-score must equal the formula. A universe z-score
-- of style_exposures must equal it too, which proves that the parameters are those of the
-- estimation.
{% set styles = {
    'size': 'case when u.market_cap > 0 then ln(u.market_cap) end',
    'liquidity': 'case when u.adv > 0 and u.market_cap > 0 then ln(u.adv / u.market_cap) end',
    'beta': 's.beta_252',
    'momentum': 's.momentum_12_1',
    'reversal': 's.reversal_5',
    'volatility': 's.vol_60',
    'dividend_yield': 's.dividend_yield',
    'high_52w': 's.high_52w',
} %}
{% set weight = 'weight_cap' if var('style_center') == 'cap' else 'weight' %}
{% set clip = var('style_clip') %}

with sample as (

    select date
    from (select date, row_number() over (order by date) as t
          from (select distinct date from {{ ref('coverage_exposures') }}))
    where t % 37 = 0

), actual as (

    {% for name in styles %}
    select '{{ name }}' as style, 'coverage' as source, security_key, date, z_{{ name }} as z
    from {{ ref('coverage_exposures') }}
    where date in (select date from sample)
    union all
    select '{{ name }}', 'universe', security_key, date, z_{{ name }}
    from {{ ref('style_exposures') }}
    where date in (select date from sample)
    {{ 'union all' if not loop.last }}
    {% endfor %}

), keys as (

    select distinct security_key, date from actual

), characteristics as (

    {% for name, expr in styles.items() %}
    select '{{ name }}' as style, s.security_key, s.date, {{ expr }} as x
    from {{ ref('signals') }} s
    inner join {{ ref('int_universe') }} u on u.ticker = s.ticker and u.date = s.date
    inner join keys k on k.security_key = s.security_key and k.date = s.date
    {{ 'union all' if not loop.last }}
    {% endfor %}

), universe as (

    select c.style, c.date, c.x, e.{{ weight }} as w
    from characteristics c
    inner join {{ ref('style_exposures') }} e
        on e.security_key = c.security_key and e.date = c.date
    where isfinite(c.x)

), center as (

    select style, date, median(x) as med
    from universe
    group by style, date

), spread as (

    select u.style, u.date, c.med, 1.4826 * median(abs(u.x - c.med)) as mad
    from universe u
    inner join center c on c.style = u.style and c.date = u.date
    group by u.style, u.date, c.med

), winsorized as (

    select
        u.style, u.date, u.w, s.med, s.mad,
        case when s.mad > 0 then least(greatest(u.x, s.med - 5 * s.mad), s.med + 5 * s.mad)
             else u.x end                                           as v
    from universe u
    inner join spread s on s.style = u.style and s.date = u.date

), parameters as (

    select
        style, date, any_value(med) as med, any_value(mad) as mad,
        sum(w * v) / sum(w)                                         as mu,
        stddev_pop(v)                                               as sd,
        count(*)                                                    as n
    from winsorized
    group by style, date

), expected as (

    select
        c.style, c.security_key, c.date,
        case when isfinite(c.x) and p.n >= 3 and p.sd > 0
             then greatest(least(
                    ((case when p.mad > 0
                           then least(greatest(c.x, p.med - 5 * p.mad), p.med + 5 * p.mad)
                           else c.x end) - p.mu) / p.sd,
                    {{ clip }}), -{{ clip }})
             else 0 end                                             as z
    from characteristics c
    -- A session with fewer than 3 finite values in the universe has no parameters, and the
    -- z-score is 0 (beta and momentum in the first sessions).
    left join parameters p on p.style = c.style and p.date = c.date

)

select a.source, a.style, a.security_key, a.date, a.z as actual_z, e.z as expected_z
from actual a
left join expected e
    on e.style = a.style and e.security_key = a.security_key and e.date = a.date
where e.z is null or abs(a.z - e.z) > 1e-9

{#
  What moved the in-universe market on each session of the last year, from the style and
  industry model. The market is cap-weighted (each weight the cap of the session before)
  or equal-weighted. Its return is split two ways, and each way sums to the return:

  - kind factor: the market factor, the industries together, each style (the exposures
    of the session before times the style return) and the stock-specific part.
  - kind industry: the return of the names of each Fama-French 12 industry, times their
    weight.

  Under cap weights the industries sum to 0 (the constraint of the model) and each style
  to about 0 (the z-scores are centered on the cap), so the cap-weighted market splits
  into the market factor and the stock-specific part. The residuals are neutral under the
  root of the cap, not the cap, so that part is not 0. The equal-weighted market holds
  more small names, so its industries and styles show.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}

with last_session as (

    select max(date) as d from {{ ref('style_residuals') }}

), days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('signals') }})

), returns as (

    select date, security_key, coalesce(industry, 'Unknown') as industry, weight_cap, ret,
           market_part, coalesce(industry_part, 0) as industry_part, resid
    from {{ ref('style_residuals') }}
    where date > (select d from last_session) - interval 1 year and weight_cap > 0

), exposures as (

    -- The exposures of the session before, dated on the session of the return.
    select d.next_date as date, e.security_key,
           {% for s in styles %}e.z_{{ s }}{{ "," if not loop.last }}{% endfor %}
    from {{ ref('style_exposures') }} e
    join days d on d.date = e.date
    where d.next_date > (select d from last_session) - interval 1 year

), names as (

    select
        r.*,
        r.weight_cap / sum(r.weight_cap) over (partition by r.date) as w_cap,
        1.0 / count(*) over (partition by r.date)                   as w_eq,
        {% for s in styles %}x.z_{{ s }} * f.{{ s }} as {{ s }}{{ "," if not loop.last }}
        {% endfor %}
    from returns r
    join exposures x using (date, security_key)
    join {{ ref('style_factor_returns') }} f using (date)

), factor_parts as (

    unpivot (
        {% for weighting, w in [('cap', 'w_cap'), ('equal', 'w_eq')] %}
        select date, '{{ weighting }}' as weighting,
               sum({{ w }} * market_part) as market,
               sum({{ w }} * industry_part) as industries,
               {% for s in styles %}sum({{ w }} * {{ s }}) as {{ s }},
               {% endfor %}sum({{ w }} * resid) as specific
        from names
        group by date
        {{ "union all" if not loop.last }}
        {% endfor %})
    on columns(* exclude (date, weighting))
    into name component value contribution

)

select date, weighting, 'factor' as kind, component, contribution
from factor_parts
union all
select date, 'cap', 'industry', lower(industry), sum(w_cap * ret)
from names
group by date, industry
union all
select date, 'equal', 'industry', lower(industry), sum(w_eq * ret)
from names
group by date, industry
order by date, weighting, kind, component

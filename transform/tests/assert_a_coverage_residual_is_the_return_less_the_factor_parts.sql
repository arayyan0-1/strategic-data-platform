-- Rebuild the parts of the return of a sample of sessions from the exposures and the factor
-- returns of the next session: the market, the industry of the name and the sum of z times
-- the style return. The residual is the return less the three parts. A row with a return and
-- factor returns must have a residual, and no residual stands without a row.
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}

with days as (

    select date, lead(date) over (order by date) as next_date, row_number() over (order by date) as t
    from (select distinct date from {{ ref('signals') }})

), expected as (

    select
        e.security_key,
        f.date,
        e.fwd_ret_1                                                 as ret,
        f.market                                                    as market_part,
        case e.industry
            {% for c in industries %}when '{{ c }}' then f.ind_{{ c | lower }}
            {% endfor %}end                                         as industry_part,
        {% for s in styles %}e.z_{{ s }} * f.{{ s }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                    as style_part
    from {{ ref('coverage_exposures') }} e
    inner join days d on d.date = e.date
    inner join {{ ref('style_factor_returns') }} f on f.date = d.next_date
    where e.fwd_ret_1 is not null and f.market is not null and d.t % 7 = 0

)

select x.security_key, x.date, r.resid
from expected x
left join {{ ref('coverage_residuals') }} r
    on r.security_key = x.security_key and r.date = x.date
where r.security_key is null
   or abs(r.ret - x.ret) > 1e-12
   or abs(r.market_part - x.market_part) > 1e-12
   or abs(coalesce(r.industry_part, 0) - coalesce(x.industry_part, 0)) > 1e-12
   or abs(r.style_part - x.style_part) > 1e-12
   or abs(x.ret - x.market_part - coalesce(x.industry_part, 0) - x.style_part - r.resid) > 1e-12

union all

select r.security_key, r.date, r.resid
from {{ ref('coverage_residuals') }} r
where abs(r.ret - r.market_part - coalesce(r.industry_part, 0) - r.style_part - r.resid) > 1e-12

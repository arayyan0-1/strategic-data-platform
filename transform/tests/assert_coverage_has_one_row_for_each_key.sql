-- One row per security and session in each coverage table, and every z-score inside the clip.
select 'duplicate exposure key' as problem, security_key, date
from {{ ref('coverage_exposures') }}
group by security_key, date
having count(*) > 1

union all

select 'duplicate residual key', security_key, date
from {{ ref('coverage_residuals') }}
group by security_key, date
having count(*) > 1

union all

select 'z-score beyond the clip', security_key, date
from {{ ref('coverage_exposures') }}
where greatest(
    {% for s in ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}abs(z_{{ s }}){{ ', ' if not loop.last }}{% endfor %}
    ) > {{ var('style_clip') }} + 1e-9

{# A returned row means the style market factor or the first eigenportfolio does not track the
   cap-weighted universe return (correlation under 0.9 or 0.8): a sign error, date shift or lost
   weight. #}
with r as (
    select b.date, b.cap_ret, s.market, p.pc1
    from {{ ref('market_breadth') }} b
    join {{ ref('style_factor_returns') }} s using (date)
    join {{ ref('pca_factor_returns') }} p using (date)
)
select corr(cap_ret, market) as market_corr, corr(cap_ret, pc1) as pc1_corr
from r
having corr(cap_ret, market) < 0.9 or corr(cap_ret, pc1) < 0.8

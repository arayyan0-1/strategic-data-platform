-- The market style factor and the first eigenportfolio are both a market portfolio, so
-- each must track the dollar-volume-weighted return of the universe. A sign error, a
-- date shift or a lost weight breaks the correlation.
with r as (
    select b.date, b.dv_ret, s.market, p.pc1
    from {{ ref('market_breadth') }} b
    join {{ ref('style_factor_returns') }} s using (date)
    join {{ ref('pca_factor_returns') }} p using (date)
)
select corr(dv_ret, market) as market_corr, corr(dv_ret, pc1) as pc1_corr
from r
having corr(dv_ret, market) < 0.9 or corr(dv_ret, pc1) < 0.8

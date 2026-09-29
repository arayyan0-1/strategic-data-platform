-- Each industry return of the style model is the industry less the market, and so is
-- each published Fama-French industry portfolio less the market. The two differ in
-- weights (the square root of cap against cap) and in universe, so the correlation is
-- well below 1, but a wrong mapping of the SIC codes or a shift of one day would drop
-- it to near 0.
select a.factor, corr(a.ret, b.ret) as corr, count(*) as n_days
from {{ ref('factor_returns') }} a
join {{ ref('factor_returns') }} b
  on b.date = a.date and b.factor = a.factor and b.family = 'ff_industry'
where a.family = 'industry'
group by a.factor
having count(*) < 250 or corr(a.ret, b.ret) < 0.4

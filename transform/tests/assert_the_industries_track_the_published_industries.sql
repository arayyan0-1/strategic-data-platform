{# A returned row is an industry whose return correlates under 0.4 with the published Fama-French
   industry less the market, or has under 250 days. The weights and universe differ, so the
   correlation is well below 1; a wrong SIC map or date shift drops it to near 0. #}
select a.factor, corr(a.ret, b.ret) as corr, count(*) as n_days
from {{ ref('factor_returns') }} a
join {{ ref('factor_returns') }} b
  on b.date = a.date and b.factor = a.factor and b.family = 'ff_industry'
where a.family = 'industry'
group by a.factor
having count(*) < 250 or corr(a.ret, b.ret) < 0.4

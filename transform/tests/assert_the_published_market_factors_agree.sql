{# A returned row means the q-factor market and the Fama-French market (both US market less the
   risk-free rate) correlate under 0.98 or share under 250 days: a date shift in an ingest. #}
select corr(a.ret, b.ret) as corr, count(*) as n_days
from {{ ref('factor_returns') }} a
join {{ ref('factor_returns') }} b using (date)
where a.family = 'q' and a.factor = 'mkt' and b.family = 'ff' and b.factor = 'mkt_rf'
having count(*) < 250 or corr(a.ret, b.ret) < 0.98

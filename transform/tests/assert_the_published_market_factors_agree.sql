-- The q-factor market and the Fama-French market are both the value-weighted US market
-- less the risk-free rate, from two sources. A date shift in either ingest drops the
-- correlation far below 1.
select corr(a.ret, b.ret) as corr, count(*) as n_days
from {{ ref('factor_returns') }} a
join {{ ref('factor_returns') }} b using (date)
where a.family = 'q' and a.factor = 'mkt' and b.family = 'ff' and b.factor = 'mkt_rf'
having count(*) < 250 or corr(a.ret, b.ret) < 0.98

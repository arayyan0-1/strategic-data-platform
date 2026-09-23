-- Dividend yield is trailing cash over price, so it is never negative and, for a
-- payer, sits in a sane annual band. A units error (cash not divided by price, or
-- a split blowup inside the window) would move the median far out. Check payers
-- on recent dense sessions.
with last as (select max(date) as d from {{ ref('mart_signals') }})

select
    s.date,
    median(s.div_yield) as median_yield,
    count(*)            as n_payers
from {{ ref('mart_signals') }} s, last
where s.in_universe
  and s.div_yield > 0
  and s.date > last.d - interval 90 day
group by s.date
having count(*) >= 20
   and (median(s.div_yield) <= 0 or median(s.div_yield) > 0.15)

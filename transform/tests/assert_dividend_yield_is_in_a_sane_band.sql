{# A returned row is a recent session where the median payer yield is not in (0, 0.15]: a units
   or split error. #}
with last as (select max(date) as d from {{ ref('signals') }})

select
    s.date,
    median(s.dividend_yield) as median_yield,
    count(*)            as n_payers
from {{ ref('signals') }} s, last
where s.in_universe
  and s.dividend_yield > 0
  and s.date > last.d - interval 90 day
group by s.date
having count(*) >= 20
   and (median(s.dividend_yield) <= 0 or median(s.dividend_yield) > 0.15)

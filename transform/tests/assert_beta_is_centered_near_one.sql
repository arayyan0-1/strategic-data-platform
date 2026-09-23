-- Beta is measured against the equal-weight market of the same in-universe
-- names, so the typical in-universe beta must sit near 1. A sign error or a
-- wrong window would move it far off. Check the median over recent dense
-- sessions; a wide band keeps the test about gross errors, not calibration.
with last as (select max(date) as d from {{ ref('mart_signals') }})

select
    s.date,
    median(s.beta_252) as median_beta,
    count(*)           as n
from {{ ref('mart_signals') }} s, last
where s.in_universe
  and s.beta_252 is not null
  and s.date > last.d - interval 90 day
group by s.date
having count(*) >= {{ var('ic_min_names') }}
   and (median(s.beta_252) < 0.5 or median(s.beta_252) > 1.5)

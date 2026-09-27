-- Beta is measured against the cap-weighted market of the in-universe names, so the
-- cap-weighted mean beta of those names must sit near 1. A sign error, a wrong window
-- or a wrong weight would move it far off. The band is wide: the weights drift over
-- the year of the window, and the test is about gross errors, not calibration.
with last as (select max(date) as d from {{ ref('signals') }})

select
    s.date,
    sum(s.beta_252 * u.market_cap) / sum(u.market_cap) as cap_weighted_beta,
    count(*)                                            as n_names
from {{ ref('signals') }} s
join {{ ref('int_universe') }} u on u.ticker = s.ticker and u.date = s.date, last
where s.in_universe
  and s.beta_252 is not null
  and u.market_cap > 0
  and s.date > last.d - interval 90 day
group by s.date
having count(*) >= {{ var('ic_min_names') }}
   and (sum(s.beta_252 * u.market_cap) / sum(u.market_cap) < 0.8
        or sum(s.beta_252 * u.market_cap) / sum(u.market_cap) > 1.2)

{# A returned row is a recent session where the cap-weighted mean beta of the universe is outside
   0.8 to 1.2. The band is wide: it catches gross errors (sign, window, weight), not calibration. #}
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

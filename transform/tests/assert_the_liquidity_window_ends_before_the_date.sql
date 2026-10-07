{# A returned row is an adv that differs from a recomputation over the window ending at D-1. A
   window including D would use the volume of the day the position opens. Relative tolerance, as
   a sliding sum differs in its last bits between runs. #}
with recomputed as (
    select
        l.ticker,
        l.date,
        avg(p.dollar_volume) over (
            partition by l.security_key order by l.date
            rows between {{ var('adv_window') }} preceding and 1 preceding
        ) as adv_excluding_today
    from {{ ref('int_security_lines') }} l
    inner join {{ ref('int_prices_adjusted') }} p
        on p.ticker = l.ticker
       and p.date   = l.date
    where l.is_primary_line
)
select u.ticker, u.date, u.adv, r.adv_excluding_today
from {{ ref('int_universe') }} u
inner join recomputed r
    on u.ticker = r.ticker
   and u.date   = r.date
where (u.adv is null) <> (r.adv_excluding_today is null)
   or coalesce(abs(u.adv - r.adv_excluding_today) > 1e-9 * abs(r.adv_excluding_today), false)

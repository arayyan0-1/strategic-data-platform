-- The trailing window must end at D-1. A window that includes D uses the volume
-- of the day on which the position opens, which the strategy cannot know.
-- This test recomputes the average a second time, from the primary line of each
-- security, and compares. A change of the frame to "current row" makes it fail on
-- almost every row. A sliding sum can differ in its last bits between two runs, so
-- the comparison has a relative tolerance.
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

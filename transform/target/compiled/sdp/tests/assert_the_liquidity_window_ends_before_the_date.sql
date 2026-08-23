-- The trailing window must end at D-1. A window that includes D uses the volume
-- of the day on which the position opens, which the strategy cannot know.
-- This test recomputes the average a second time and compares. A change of the
-- frame to "current row" makes it fail on almost every row.
with recomputed as (
    select
        ticker,
        date,
        avg(dollar_volume) over (
            partition by ticker order by date
            rows between 20 preceding and 1 preceding
        ) as adv_excluding_today
    from "sdp"."main_staging"."stg_prices_adjusted"
)
select u.ticker, u.date, u.adv, r.adv_excluding_today
from "sdp"."main_staging"."stg_universe" u
join recomputed r
  on u.ticker = r.ticker
 and u.date   = r.date
where u.adv is distinct from r.adv_excluding_today
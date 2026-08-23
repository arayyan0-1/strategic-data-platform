-- The gap between the settlement date and the effective date must be exactly
-- the configured number of XNYS sessions.
--
-- Counting in sessions and not in calendar days is the point. A market holiday
-- inside the window would shorten a calendar-day lag and let a signal read the
-- number before it was public.
with sessions as (

    select date as session
    from (select distinct date from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/us_stocks_day_aggs/date=*/data.parquet',
        hive_partitioning = true
    ))

),

measured as (

    select
        s.settlement_date,
        s.effective_date,
        (select count(*) from sessions c
          where c.session > s.settlement_date
            and c.session <= s.effective_date) as sessions_between
    from (select distinct settlement_date, effective_date
          from "sdp"."main_staging"."stg_short_interest"
          where effective_date is not null) s

)

select *
from measured
where sessions_between <> 8
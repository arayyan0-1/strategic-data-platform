-- The gap between the settlement date and the effective date must be exactly
-- the configured number of XNYS sessions.
--
-- Counting in sessions and not in calendar days is the point. A market holiday
-- inside the window would shorten a calendar-day lag and let a signal read the
-- number before it was public.
with sessions as (

    select date as session
    from (select distinct date from {{ lake('us_stocks_day_aggs') }})

),

measured as (

    select
        s.settlement_date,
        s.effective_date,
        (select count(*) from sessions c
          where c.session > s.settlement_date
            and c.session <= s.effective_date) as sessions_between
    from (select distinct settlement_date, effective_date
          from {{ ref('stg_short_interest') }}
          where effective_date is not null) s

)

select *
from measured
where sessions_between <> {{ var('short_interest_publication_sessions') }}

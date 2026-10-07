{# Short interest. Join on effective_date (the first session the number was public), never on
   settlement_date. days_to_cover is null where the vendor caps it at 999.99;
   days_to_cover_computed is uncensored. #}

with sessions as (

    select
        date as session,
        row_number() over (order by date) as rn
    from (select distinct date from {{ ref('stg_massive__day_aggs') }})

),

raw as (

    select
        settlement_date,
        ticker,
        short_interest,
        avg_daily_volume,
        days_to_cover
    from {{ ref('stg_massive__short_interest') }}

),

-- First session after each settlement date, for dates the bars cover, so the count never
-- starts from the first bar.
first_session_after as (

    select
        r.settlement_date,
        min(s.rn) as rn_first
    from (select distinct settlement_date from raw) r
    join sessions s
      on s.session > r.settlement_date
    where r.settlement_date >= (select min(session) from sessions)
    group by 1

),

publication as (

    select
        f.settlement_date,
        s.session as effective_date
    from first_session_after f
    left join sessions s
      on s.rn = f.rn_first + {{ var('short_interest_publication_sessions') }} - 1

)

select
    r.settlement_date,
    p.effective_date,
    r.ticker,
    r.short_interest,
    r.avg_daily_volume,

    -- The 999.99 cap is a censor, not a measurement.
    case when r.days_to_cover < 999.99 then r.days_to_cover end as days_to_cover,
    r.days_to_cover >= 999.99                                   as days_to_cover_is_censored,

    -- Uncensored ratio from the raw columns, null only where volume is zero.
    r.short_interest / nullif(r.avg_daily_volume, 0)            as days_to_cover_computed

from raw r
left join publication p
    on p.settlement_date = r.settlement_date

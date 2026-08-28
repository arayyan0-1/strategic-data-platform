{#
  Short interest, with two rules. (1) FINRA publishes a settlement date about
  eight sessions later, so `effective_date` marks the first session the number
  was public. Join on it, never on settlement_date. It is null unless the bars
  cover the settlement date at both ends. (2) days_to_cover is null where the
  vendor caps it at 999.99 (not a measurement); use days_to_cover_computed.
#}

with sessions as (

    select
        date as session,
        row_number() over (order by date) as rn
    from (select distinct date from {{ lake('us_stocks_day_aggs') }})

),

raw as (

    select
        cast(settlement_date as date) as settlement_date,
        ticker,
        short_interest,
        avg_daily_volume,
        days_to_cover
    from {{ lake('massive_short_interest') }}

),

-- First session after each settlement date. Restricted to settlement dates the
-- bars cover, so the count never starts from the first bar.
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

    -- Null where censored, because the 999.99 cap is not a measurement.
    case when r.days_to_cover < 999.99 then r.days_to_cover end as days_to_cover,
    r.days_to_cover >= 999.99                                   as days_to_cover_is_censored,

    -- Uncensored ratio from the raw columns, null only where volume is zero.
    r.short_interest / nullif(r.avg_daily_volume, 0)            as days_to_cover_computed

from raw r
left join publication p
    on p.settlement_date = r.settlement_date

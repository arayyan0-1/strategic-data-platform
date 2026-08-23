{#
  Short interest, with the two rules that decision 0009 requires.

  1. The publication lag. FINRA disseminates a settlement date about eight
     sessions later. The raw partition is keyed on the settlement date, so a
     study that reads it on that date uses a number that nobody had. This model
     adds `effective_date`, which is the first session on which the number was
     public. Join a signal on `effective_date` and never on `settlement_date`.

     The lag is counted in XNYS sessions and the session list comes from the
     bars, so a market holiday cannot shorten it.

     `effective_date` is null unless the bar calendar covers the settlement date
     at both ends. The upper guard is obvious. The lower guard is not, and it
     matters more: without it, a settlement date before the first bar counts its
     lag from the first bar instead of from itself, and every such settlement
     collapses onto the same wrong date. That date looks reasonable, which is
     worse than an error. Null is the honest answer, and the backfill removes
     the case.

  2. The censored days_to_cover. The vendor caps this field at 999.99. Decision
     0009 recorded the cap as a sentinel for `avg_daily_volume = 0`, and that is
     incomplete. Measured on four settlement dates: 15,317 rows hold 999.99 and
     only 12,074 of them have zero volume. The other 3,243 have an implied
     ratio at or above the cap, from 1,001 to 21.4 million, while the largest
     value below the cap is 998.82. So 999.99 means "at or above 999.99" and it
     is never a measurement.

     The field is therefore null in this model and a flag records the censoring.
     `days_to_cover_computed` holds the ratio that the two raw columns give,
     which is uncensored wherever volume is above zero.
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

-- The first session strictly after each settlement date. Restricted to the
-- settlement dates that the bar calendar actually covers, so that the count
-- never starts from the first bar instead of from the settlement.
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

    -- Null, because the cap is not a measurement. See the header.
    case when r.days_to_cover < 999.99 then r.days_to_cover end as days_to_cover,
    r.days_to_cover >= 999.99                                   as days_to_cover_is_censored,

    -- The ratio computed from the two raw columns. Uncensored, and null only
    -- where the vendor reports no volume at all.
    r.short_interest / nullif(r.avg_daily_volume, 0)            as days_to_cover_computed

from raw r
left join publication p
    on p.settlement_date = r.settlement_date

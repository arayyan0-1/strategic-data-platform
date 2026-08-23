{#
  One row for each (ticker, event_date, kind).

  The vendor gives one row for each event. Two rows can carry the same ticker and
  the same date. For dividends this is often real, because a special distribution
  and a recurring distribution can share an ex-date. For splits it is a vendor
  contradiction, because a cumulative factor is a property of the date.

  Measured on the pull of 2026-08-22:
    splits     211 duplicate (ticker, execution_date) pairs. All 211 disagree on
               the factor. 137 also disagree on adjustment_type.
    dividends  13,926 duplicate (ticker, ex_dividend_date) pairs, of which 3,835
               have an ex-date inside the price window.

  An as-of join against the raw rows therefore makes one price row into two, with
  two different adjusted prices. This model collapses the duplicates first and
  records the disagreement, so that the exposure is measurable and not hidden.
  See docs/decisions/0010.
#}

with splits_src as (

    select
        ticker,
        cast(execution_date as date)          as event_date,
        split_from,
        split_to,
        historical_adjustment_factor          as factor,
        adjustment_type
    from {{ lake('massive_splits', 'pull_date') }}
    where pull_date = {{ ca_pull_date('massive_splits') }}
      and ticker is not null
      and execution_date is not null
      -- A null factor on a split is fatal at ingest, so a null here cannot occur.
      -- A factor of 0 is RYCEF and it has no valid adjustment.
      and historical_adjustment_factor > 0
      and split_from > 0
      and split_to > 0

), splits_grouped as (

    select
        ticker,
        event_date,
        'split'                                       as kind,
        max(factor)                                   as factor,
        arg_max(split_from / split_to, factor)        as ratio,
        count(*)                                      as n_events,
        count(distinct factor)                        as n_distinct_factors,
        max(factor) / min(factor)                     as factor_spread,
        false                                         as factor_is_null
    from splits_src
    group by 1, 2

), dividends_src as (

    select
        ticker,
        cast(ex_dividend_date as date)        as event_date,
        historical_adjustment_factor          as factor,
        cash_amount
    from {{ lake('massive_dividends', 'pull_date') }}
    where pull_date = {{ ca_pull_date('massive_dividends') }}
      and ticker is not null
      and ex_dividend_date is not null
      and currency = 'USD'
      -- Keep the null factor. It is structural: the vendor has no price on the
      -- ex-date for that security. A predicate of "factor > 0" alone removes the
      -- null rows without a message, and the as-of join then reports 1.0 for a
      -- date whose factor is unknown. Three-valued logic, the same trap as the
      -- cash_amount predicate in the audits.
      and (historical_adjustment_factor is null or historical_adjustment_factor > 0)

), dividends_grouped as (

    select
        ticker,
        event_date,
        'dividend'                                    as kind,
        -- One null distribution makes the date unknown. It does not make it 1.0.
        case when bool_or(factor is null) then null else min(factor) end as factor,
        cast(null as double)                          as ratio,
        count(*)                                      as n_events,
        count(distinct factor)                        as n_distinct_factors,
        max(factor) / nullif(min(factor), 0)          as factor_spread,
        bool_or(factor is null)                       as factor_is_null
    from dividends_src
    group by 1, 2

), unioned as (

    select * from splits_grouped
    union all
    select * from dividends_grouped

)

select
    ticker,
    event_date,
    kind,
    factor,
    ratio,
    n_events,
    n_distinct_factors,
    factor_spread,
    factor_is_null,
    n_events > 1 as is_collapsed,
    {#
      True when this event, or any later event on the same ticker, had its rows
      collapsed. The vendor factor is cumulative, so an ambiguity at one date
      travels backwards through every earlier factor of that ticker. Measured on
      the pull of 2026-08-09: the chained check disagrees with the vendor factor
      on 170 split events, and all 170 have a collapsed event at or after them.
      There is no case of a disagreement without one.
    #}
    bool_or(n_events > 1) over (
        partition by ticker, kind
        order by event_date desc
        rows between unbounded preceding and current row
    ) as has_collapsed_at_or_after,
    {#
      The validation column. The vendor factor is cumulative and it includes the
      event itself: factor(e) = product of split_from/split_to over every event
      with a date at or after e. This column reproduces that product from the
      ratio components. It is a check on the semantics and it is never a
      substitute for the vendor factor. See docs/decisions/0005.
    #}
    case when kind = 'split' then
        exp(sum(ln(ratio)) over (
            partition by ticker, kind
            order by event_date desc
            rows between unbounded preceding and current row
        ))
    end as factor_chained_check
from unioned

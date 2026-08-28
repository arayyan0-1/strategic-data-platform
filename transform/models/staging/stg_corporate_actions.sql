{#
  One row per (ticker, event_date, kind). The vendor can give two rows for one
  key that disagree on the factor. Collapse them and record the disagreement, so
  an as-of join cannot turn one price row into two.
#}

with splits_src as (

    select
        ticker,
        cast(execution_date as date)          as event_date,
        split_from,
        split_to,
        historical_adjustment_factor          as factor,
        adjustment_type
    from {{ ca_table('massive_splits') }}
    where ticker is not null
      and execution_date is not null
      -- factor is null (fatal at ingest, so absent here) or 0 (RYCEF, no valid
      -- adjustment). Drop both.
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
    from {{ ca_table('massive_dividends') }}
    where ticker is not null
      and ex_dividend_date is not null
      and currency = 'USD'
      -- Keep the null factor. It is structural (no vendor price on the ex-date).
      -- `factor > 0` alone would drop nulls silently, and the as-of join would
      -- then report 1.0 for an unknown date.
      and (historical_adjustment_factor is null or historical_adjustment_factor > 0)

), dividends_grouped as (

    select
        ticker,
        event_date,
        'dividend'                                    as kind,
        -- One null distribution makes the date unknown, not 1.0.
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
    -- True when this event, or any later one on the same ticker, was collapsed.
    -- The factor is cumulative, so an ambiguity travels back through earlier dates.
    bool_or(n_events > 1) over (
        partition by ticker, kind
        order by event_date desc
        rows between unbounded preceding and current row
    ) as has_collapsed_at_or_after,
    -- Validation only: reproduce the cumulative split factor from the ratios.
    -- Never a substitute for the vendor factor.
    case when kind = 'split' then
        exp(sum(ln(ratio)) over (
            partition by ticker, kind
            order by event_date desc
            rows between unbounded preceding and current row
        ))
    end as factor_chained_check
from unioned

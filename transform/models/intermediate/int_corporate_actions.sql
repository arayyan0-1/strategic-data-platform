{# One row per (ticker, event_date, kind). Duplicate vendor rows that disagree on the factor are
   collapsed and the disagreement recorded, so a join cannot turn one price row into two. #}

with splits_src as (

    select
        ticker,
        cast(execution_date as date)          as event_date,
        split_from,
        split_to,
        historical_adjustment_factor          as factor,
        adjustment_type
    from {{ ref('stg_massive__splits') }}
    where ticker is not null
      and execution_date is not null
      -- Drop a null factor (fatal at ingest, so absent) and a zero factor (no valid adjustment).
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
        false                                         as factor_is_null,
        cast(null as double)                          as cash_amount,
        cast(null as double)                          as regular_cash_amount
    from splits_src
    group by 1, 2

), dividends_src as (

    select
        ticker,
        cast(ex_dividend_date as date)        as event_date,
        historical_adjustment_factor          as factor,
        cash_amount,
        distribution_type
    from {{ ref('stg_massive__dividends') }}
    where ticker is not null
      and ex_dividend_date is not null
      and currency = 'USD'
      -- Keep the null factor: it is structural (no vendor price on the ex-date). `factor > 0`
      -- alone would drop it silently and the join would report 1.0 for an unknown date.
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
        bool_or(factor is null)                       as factor_is_null,
        -- Per-share cash on the ex-date. Every row counts, specials included.
        max(cash_amount)                              as cash_amount,
        -- Same, from the rows not marked special; feeds the dividend yield. A row with no type
        -- is regular. Null when every row is special. Max, not sum: the vendor can repeat a row.
        max(cash_amount) filter (where distribution_type is distinct from 'special')
                                                      as regular_cash_amount
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
    cash_amount,
    regular_cash_amount,
    n_events > 1 as is_collapsed,
    -- True when this or any later event on the ticker was collapsed. The factor is cumulative,
    -- so an ambiguity travels back through earlier dates.
    bool_or(n_events > 1) over (
        partition by ticker, kind
        order by event_date desc
        rows between unbounded preceding and current row
    ) as has_collapsed_at_or_after,
    -- Validation only: the cumulative split factor rebuilt from the ratios.
    case when kind = 'split' then
        exp(sum(ln(ratio)) over (
            partition by ticker, kind
            order by event_date desc
            rows between unbounded preceding and current row
        ))
    end as factor_chained_check
from unioned

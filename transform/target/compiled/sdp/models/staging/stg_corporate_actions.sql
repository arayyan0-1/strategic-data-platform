

with splits_src as (

    select
        ticker,
        cast(execution_date as date)          as event_date,
        split_from,
        split_to,
        historical_adjustment_factor          as factor,
        adjustment_type
    from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_splits/pull_date=*/data.parquet',
        hive_partitioning = true
    )
    where pull_date = (select min(pull_date) from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_splits/pull_date=*/data.parquet',
        hive_partitioning = true
    ))
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
    from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_dividends/pull_date=*/data.parquet',
        hive_partitioning = true
    )
    where pull_date = (select min(pull_date) from read_parquet(
        '/Users/aray/strategic-data-platform/data/raw/massive_dividends/pull_date=*/data.parquet',
        hive_partitioning = true
    ))
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
    
    bool_or(n_events > 1) over (
        partition by ticker, kind
        order by event_date desc
        rows between unbounded preceding and current row
    ) as has_collapsed_at_or_after,
    
    case when kind = 'split' then
        exp(sum(ln(ratio)) over (
            partition by ticker, kind
            order by event_date desc
            rows between unbounded preceding and current row
        ))
    end as factor_chained_check
from unioned
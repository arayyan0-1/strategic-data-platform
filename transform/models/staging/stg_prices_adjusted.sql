{#
  Adjusted prices from two as-of joins. The vendor factor is cumulative, so the
  adjustment is one lookup: for a price on D, take the first event strictly after
  D and multiply. Strictly after, not on, or every split gets one false return.
  Splits and dividends are independent factors and multiply.
#}

with bars as (

    select
        ticker,
        cast(date as date)  as date,
        open, high, low, close, volume, transactions
    from {{ lake('us_stocks_day_aggs', 'date') }}
    where ticker is not null

-- Bound events at the last bar. A later event has not happened inside the panel
-- and must not restate it, and a future ex-date has a null factor by construction.
), panel_end as (

    select max(date) as last_bar_date from bars

), splits as (

    select ticker, event_date, factor, is_collapsed
    from {{ ref('stg_corporate_actions') }}
    where kind = 'split'
      and event_date <= (select last_bar_date from panel_end)

), dividends as (

    select ticker, event_date, factor, is_collapsed
    from {{ ref('stg_corporate_actions') }}
    where kind = 'dividend'
      and event_date <= (select last_bar_date from panel_end)

), with_split as (

    select
        b.*,
        s.event_date    as next_split_date,
        s.factor        as next_split_factor,
        s.is_collapsed  as split_is_collapsed
    from bars b
    asof left join splits s
      on b.ticker = s.ticker
     and b.date < s.event_date

), with_both as (

    select
        w.*,
        d.event_date    as next_dividend_date,
        d.factor        as next_dividend_factor,
        d.is_collapsed  as dividend_is_collapsed
    from with_split w
    asof left join dividends d
      on w.ticker = d.ticker
     and w.date < d.event_date

), factors as (

    select
        *,
        -- No later event is a factor of 1.0. A later event with a null factor is
        -- unknown, not 1.0. Keep the two apart.
        case when next_split_date is null then 1.0 else next_split_factor end
            as split_factor,
        case when next_dividend_date is null then 1.0 else next_dividend_factor end
            as dividend_factor
    from with_both

)

select
    ticker,
    date,

    -- Unadjusted price. A price floor screens on the traded price.
    open, high, low, close, volume, transactions,
    close * volume                              as dollar_volume,

    split_factor,
    dividend_factor,
    split_factor * dividend_factor              as total_factor,

    -- Split-adjusted: mechanical and complete.
    open  * split_factor                        as adj_open_split,
    high  * split_factor                        as adj_high_split,
    low   * split_factor                        as adj_low_split,
    close * split_factor                        as adj_close_split,
    volume / split_factor                       as adj_volume,

    -- Total-adjusted: the default column, null where the dividend factor is unknown.
    close * split_factor * dividend_factor      as adj_close_total,

    next_split_date,
    next_dividend_date,
    coalesce(split_is_collapsed, false)
        or coalesce(dividend_is_collapsed, false)  as factor_from_collapsed_event

from factors

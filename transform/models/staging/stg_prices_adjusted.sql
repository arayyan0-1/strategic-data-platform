{#
  Adjusted prices, built with two as-of joins.

  The vendor factor is cumulative and it already contains every later action, so
  the adjustment is one lookup and not a chained product. For a price on date D,
  take the first event with a date strictly after D and multiply by its factor.

  Strictly after, and not on. A split applies overnight. On the execution date all
  trading is already adjusted. An error of one day here gives one very large false
  return for each split and each name.

  Splits and dividends are independent factors. They multiply. The two as-of joins
  are therefore separate, and the two action tables are never joined to each other.

  See docs/decisions/0005.
#}

with bars as (

    select
        ticker,
        cast(date as date)  as date,
        open, high, low, close, volume, transactions
    from {{ lake('us_stocks_day_aggs', 'date') }}
    where ticker is not null

{#
  An event after the last bar has not happened inside the panel, so it must not
  restate the panel. Its factor is also null by construction for a dividend,
  because the vendor computes the dividend factor from the price on the ex-date
  and that price does not exist yet.

  Without this bound, every liquid name that has an announced ex-date in the next
  weeks gets a null adj_close_total. Measured on the pull of 2026-08-09 against
  bars to 2026-08-21: the null rate inside the universe was 14.4% and the rate
  outside it was 2.4%, which is the opposite of the true structural rate. The
  cause was future ex-dates and not missing vendor prices.

  The panel is therefore expressed on the basis of the last bar date and not of
  today. Every return inside the panel is the same under either basis, because
  the two differ by one constant for each ticker.
#}
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
        {#
          No later event means a factor of 1.0. A later event whose factor is null
          means the factor is unknown, and unknown is not 1.0. The two cases look
          the same after a coalesce, so they are separated here.
        #}
        case when next_split_date is null then 1.0 else next_split_factor end
            as split_factor,
        case when next_dividend_date is null then 1.0 else next_dividend_factor end
            as dividend_factor
    from with_both

)

select
    ticker,
    date,

    -- The unadjusted price. A price floor screens on the price at which the stock
    -- traded, and not on a value that a later event restated.
    open, high, low, close, volume, transactions,
    close * volume                              as dollar_volume,

    split_factor,
    dividend_factor,
    split_factor * dividend_factor              as total_factor,

    -- Mechanical and complete.
    open  * split_factor                        as adj_open_split,
    high  * split_factor                        as adj_high_split,
    low   * split_factor                        as adj_low_split,
    close * split_factor                        as adj_close_split,
    volume / split_factor                       as adj_volume,

    -- The default column. It is null where the dividend factor is unknown.
    close * split_factor * dividend_factor      as adj_close_total,

    next_split_date,
    next_dividend_date,
    coalesce(split_is_collapsed, false)
        or coalesce(dividend_is_collapsed, false)  as factor_from_collapsed_event

from factors

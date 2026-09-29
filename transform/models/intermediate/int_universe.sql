{#
  The universe per date, from an instrument filter and a liquidity filter. Every
  threshold is a var. The liquidity window and the seasoning count belong to a
  security, not to a ticker. They run over its primary line, so a change of ticker
  continues them, and a new security on a used ticker starts them again. They count
  rows of the line. The liquidity window ends at D-1, because a window that includes D
  uses the volume of the day a position opens. A line that is not the primary line has
  no window and fails the filter.
#}

with keyed as (

    select * from {{ ref('int_tickers_keyed') }}

), bars as (

    select
        ticker,
        date,
        close,
        adj_close_split,
        dollar_volume
    from {{ ref('int_prices_adjusted') }}

), series as (

    select l.security_key, b.ticker, b.date, b.dollar_volume
    from {{ ref('int_security_lines') }} l
    inner join bars b
        on b.ticker = l.ticker
       and b.date   = l.date
    where l.is_primary_line

), liquidity as (

    select
        ticker,
        date,
        avg(dollar_volume) over w      as adv,
        count(dollar_volume) over w    as days_in_window,
        count(*) over (
            partition by security_key order by date
            rows between unbounded preceding and current row
        )                              as bars_seen
    from series
    window w as (
        partition by security_key order by date
        rows between {{ var('adv_window') }} preceding and 1 preceding
    )

), joined as (

    select
        k.date,
        k.ticker,
        k.security_key,
        k.key_rule,
        s.is_primary_line,
        k.name,
        k.type,
        k.type_filled,
        k.primary_exchange,
        t.close,
        t.dollar_volume,
        l.adv,
        l.days_in_window,
        l.bars_seen,

        -- The market cap of the share class. It moves with the split-adjusted price
        -- from the month-end, and it is null when the month-end is over 70 days old.
        case when k.date - d.month_end <= 70
             then d.cap * t.adj_close_split / d.adj_close_split end    as market_cap,
        coalesce(d.industry, n.industry, 'Unknown')                    as industry,
        coalesce(d.industry_name, n.industry_name, 'Unknown')          as industry_name,
        coalesce(d.sic_code, n.sic_code)                               as sic_code,

        -- A null type, exchange or active flag fails the filter.
        coalesce(
            k.type_filled in ({{ sql_list('universe_types') }})
                and k.primary_exchange in ({{ sql_list('universe_exchanges') }})
                and k.active,
            false
        )                                                   as passes_instrument,

        -- A null input fails the check, so no flag is ever null.
        coalesce(t.close >= {{ var('min_price') }}, false)                  as passes_price,
        coalesce(l.adv >= {{ var('min_dollar_volume') }}, false)            as passes_adv,
        coalesce(l.days_in_window >= {{ var('min_days_in_window') }}, false) as passes_history,
        coalesce(l.bars_seen >= {{ var('min_days_since_first_bar') }}, false) as passes_seasoning

    from keyed k
    inner join bars t
        on k.ticker = t.ticker
       and k.date   = t.date
    left join liquidity l
        on k.ticker = l.ticker
       and k.date   = l.date
    inner join {{ ref('int_security_lines') }} s
        on s.ticker = k.ticker
       and s.date   = k.date
    -- The newest month-end of ticker details on or before the session.
    asof left join {{ ref('int_security_details') }} d
        on d.security_key = k.security_key
       and d.month_end   <= k.date
    -- The first month-end after, for the industry of a security that listed during
    -- the month. An industry code does not predict a return.
    asof left join {{ ref('int_security_details') }} n
        on n.security_key = k.security_key
       and n.month_end   >= k.date

), flagged as (

    select
        *,
        passes_price and passes_adv and passes_history and passes_seasoning as passes_liquidity
    from joined

)

select
    *,
    passes_instrument and passes_liquidity as in_universe
from flagged

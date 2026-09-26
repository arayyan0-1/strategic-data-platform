{#
  The universe per date, from an instrument filter and a liquidity filter. Every
  threshold is a var. The liquidity window ends at D-1, because a window that
  includes D uses the volume of the day a position opens.
#}

with keyed as (

    select * from {{ ref('stg_tickers') }}

), bars as (

    select
        ticker,
        date,
        close,
        adj_close_split,
        dollar_volume
    from {{ ref('stg_prices_adjusted') }}

), liquidity as (

    select
        ticker,
        date,
        close,
        adj_close_split,
        dollar_volume,
        avg(dollar_volume) over w      as adv,
        count(dollar_volume) over w    as days_in_window,
        count(*) over (
            partition by ticker order by date
            rows between unbounded preceding and current row
        )                              as bars_seen
    from bars
    window w as (
        partition by ticker order by date
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
        t.adv,
        t.days_in_window,
        t.bars_seen,

        -- The market cap of the share class. It moves with the split-adjusted price
        -- from the month-end, and it is null when the month-end is over 70 days old.
        case when k.date - d.snap_date <= 70
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
        coalesce(t.adv >= {{ var('min_dollar_volume') }}, false)            as passes_adv,
        coalesce(t.days_in_window >= {{ var('min_days_in_window') }}, false) as passes_history,
        coalesce(t.bars_seen >= {{ var('min_days_since_first_bar') }}, false) as passes_seasoning

    from keyed k
    inner join liquidity t
        on k.ticker = t.ticker
       and k.date   = t.date
    inner join {{ ref('stg_security_lines') }} s
        on s.ticker = k.ticker
       and s.date   = k.date
    -- The newest month-end of ticker details on or before the session.
    asof left join {{ ref('stg_security_details') }} d
        on d.security_key = k.security_key
       and d.snap_date   <= k.date
    -- The first month-end after, for the industry of a security that listed during
    -- the month. An industry code does not predict a return.
    asof left join {{ ref('stg_security_details') }} n
        on n.security_key = k.security_key
       and n.snap_date   >= k.date

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

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
        dollar_volume
    from {{ ref('stg_prices_adjusted') }}

), liquidity as (

    select
        ticker,
        date,
        close,
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

{#
  The universe per date, from an instrument filter and a liquidity filter. Every
  threshold is a var. The liquidity window ends at D-1, because a window that
  includes D uses the volume of the day a position opens.
#}

with reference as (

    select
        ticker,
        cast(date as date)      as date,
        name,
        type,
        primary_exchange,
        active,
        currency_name,
        cik,
        composite_figi,
        share_class_figi
    from {{ lake('massive_tickers', 'date') }}

), keyed as (

    select
        *,
        -- Prefer the security identifier (FIGI), fall back to an issuer id made
        -- unique by ticker. key_rule records which rule fired, so the fallback
        -- rate is measurable.
        coalesce(
            share_class_figi,
            composite_figi,
            nullif(cik, '') || '.' || ticker,
            'TICKER.' || ticker
        ) as security_key,
        case
            when share_class_figi is not null then 'share_class_figi'
            when composite_figi   is not null then 'composite_figi'
            when nullif(cik, '')  is not null then 'cik_ticker'
            else 'ticker_only'
        end as key_rule
    from reference

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
        k.name,
        k.type,
        k.primary_exchange,
        t.close,
        t.dollar_volume,
        t.adv,
        t.days_in_window,
        t.bars_seen,

        -- A null type, exchange or active flag fails the filter.
        coalesce(
            k.type in ({{ "'" ~ var('universe_types') | join("','") ~ "'" }})
                and k.primary_exchange in ({{ "'" ~ var('universe_exchanges') | join("','") ~ "'" }})
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

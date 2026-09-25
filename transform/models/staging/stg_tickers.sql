{#
  The ticker reference per date, with the security key. The universe, the security
  lines and the price adjustment all read it, so the key rule has one definition.
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
        nullif(cik, '')         as cik,
        composite_figi,
        share_class_figi
    from {{ lake('massive_tickers', 'date') }}

), filled as (

    select
        *,
        -- The vendor drops the FIGI of a name on some dates and then puts it back.
        -- Inside one ticker and one issuer (CIK), take the last earlier FIGI, or the
        -- first later one for the first dates of a name. A new CIK on the same ticker
        -- is another company and gets no fill. The key is a label, not a price input.
        case when cik is not null then coalesce(
            last_value(share_class_figi ignore nulls) over (
                partition by ticker, cik order by date
                rows between unbounded preceding and current row
            ),
            first_value(share_class_figi ignore nulls) over (
                partition by ticker, cik order by date
                rows between current row and unbounded following
            )
        ) end as share_class_figi_fill
    from reference

), keyed as (

    select
        * exclude (share_class_figi_fill),
        -- Prefer the security identifier (FIGI), fall back to an issuer id made
        -- unique by ticker. key_rule records which rule fired, so the fallback rate
        -- is measurable.
        coalesce(
            share_class_figi,
            share_class_figi_fill,
            composite_figi,
            cik || '.' || ticker,
            'TICKER.' || ticker
        ) as security_key,
        case
            when share_class_figi      is not null then 'share_class_figi'
            when share_class_figi_fill is not null then 'share_class_figi_filled'
            when composite_figi        is not null then 'composite_figi'
            when cik                   is not null then 'cik_ticker'
            else 'ticker_only'
        end as key_rule
    from filled

)

select
    *,
    -- The vendor type is null on some dates of a known name. Take the last known type
    -- of the security. The fill reads earlier dates only.
    coalesce(type, last_value(type ignore nulls) over (
        partition by security_key order by date
        rows between unbounded preceding and current row
    )) as type_filled
from keyed

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
        share_class_figi,
        -- The first word of the name. A new issuer changes it. A rename by the same
        -- fund family or company mostly keeps it.
        lower(regexp_extract(name, '[A-Za-z0-9]+')) as name_word
    from {{ lake('massive_tickers', 'date') }}

), flagged as (

    -- The vendor drops an identifier of a name on some dates, and it gives the CIK
    -- of a related issuer on others (CMSpC alternates between CMS Energy and
    -- Consumers Energy). An episode is a run of one ticker that stays with one
    -- security. A new episode starts when a FIGI changes, or when the first word of
    -- the name changes and the CIK is not the known one. A new issuer on a reused
    -- ticker can start with no CIK (STRC in July 2025).
    select
        *,
        coalesce(share_class_figi <> last_value(share_class_figi ignore nulls) over earlier, false)
        or coalesce(composite_figi <> last_value(composite_figi ignore nulls) over earlier, false)
        or coalesce(
            name_word <> last_value(name_word ignore nulls) over earlier
            and (cik is null
                 or cik <> coalesce(last_value(cik ignore nulls) over earlier, '')),
            false
        ) as starts_episode
    from reference
    window earlier as (
        partition by ticker order by date
        rows between unbounded preceding and 1 preceding
    )

), episodes as (

    select
        *,
        sum(starts_episode::int) over (
            partition by ticker order by date
            rows between unbounded preceding and current row
        ) as episode
    from flagged

), filled as (

    -- Each row takes the identifiers of its episode. The key is a label and not a
    -- price input, so a value that the vendor states on a later date of the same
    -- episode is not look-ahead.
    select
        * exclude (starts_episode),
        first_value(share_class_figi ignore nulls) over whole as episode_share_class_figi,
        first_value(composite_figi ignore nulls) over whole    as episode_composite_figi,
        first_value(cik ignore nulls) over whole               as episode_cik
    from episodes
    window whole as (
        partition by ticker, episode order by date
        rows between unbounded preceding and unbounded following
    )

), held as (

    -- Each FIGI that a row states itself, with the tickers that state it on the date.
    select share_class_figi as figi, date, min(ticker) as t1, max(ticker) as t2
    from reference where share_class_figi is not null group by 1, 2
    union all
    select composite_figi, date, min(ticker), max(ticker)
    from reference where composite_figi is not null group by 1, 2

), guarded as (

    -- A filled FIGI that another ticker states on the same date belongs to that
    -- ticker. The old ticker of a renamed company (IIVI, now COHR) can pass to a new
    -- issuer with no FIGI, and the fill must not merge the two.
    select
        f.* exclude (episode_share_class_figi, episode_composite_figi),
        case when f.share_class_figi is null and exists (
                 select 1 from held h
                 where h.figi = f.episode_share_class_figi and h.date = f.date
                   and (h.t1 <> f.ticker or h.t2 <> f.ticker))
             then null else f.episode_share_class_figi end as episode_share_class_figi,
        case when f.composite_figi is null and exists (
                 select 1 from held h
                 where h.figi = f.episode_composite_figi and h.date = f.date
                   and (h.t1 <> f.ticker or h.t2 <> f.ticker))
             then null else f.episode_composite_figi end as episode_composite_figi
    from filled f

), keyed as (

    select
        * exclude (episode_share_class_figi, episode_composite_figi, episode_cik, name_word),
        -- Prefer the security identifier (FIGI), fall back to an issuer id made
        -- unique by ticker. key_rule records which rule fired, so the fallback rate
        -- is measurable.
        coalesce(
            share_class_figi,
            episode_share_class_figi,
            composite_figi,
            episode_composite_figi,
            episode_cik || '.' || ticker,
            'TICKER.' || ticker
        ) as security_key,
        case
            when share_class_figi         is not null then 'share_class_figi'
            when episode_share_class_figi is not null then 'share_class_figi_filled'
            when coalesce(composite_figi, episode_composite_figi) is not null
                then 'composite_figi'
            when episode_cik              is not null then 'cik_ticker'
            else 'ticker_only'
        end as key_rule
    from guarded

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

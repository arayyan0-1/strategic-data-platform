{#
  The ticker reference per date, with the key of each episode. int_tickers_keyed adds the
  links between episodes and gives the security key.

  Most rows of a ticker repeat the row before them in every field that sets the key. The
  logic runs on runs of such rows and not on rows, so no window sorts all rows. A run is a
  set of consecutive rows of one ticker with the same name, CIK and FIGIs. A join gives
  each row the result of its run.
#}

with reference as not materialized (

    select
        ticker,
        date,
        name,
        type,
        primary_exchange,
        active,
        currency_name,
        cik,
        composite_figi,
        share_class_figi
    from {{ ref('stg_massive__tickers') }}

), groups as (

    -- The rows of a ticker with the same fields.
    select
        ticker, name, cik, composite_figi, share_class_figi,
        min(date) as first_date,
        max(date) as last_date,
        count(*) as n_rows
    from reference
    group by ticker, name, cik, composite_figi, share_class_figi

), mixed as (

    -- A group is a run when no other group of its ticker has a date inside it. A ticker
    -- is mixed when the fields go from one value to another and back (a FIGI that the
    -- vendor drops for one session).
    select ticker
    from (
        select
            ticker,
            last_date,
            lead(first_date) over (partition by ticker order by first_date) as next_first_date
        from groups
    )
    where last_date >= next_first_date
    group by ticker

), mixed_rows as (

    select
        r.ticker, r.date, r.name, r.cik, r.composite_figi, r.share_class_figi,
        (lag(r.date) over w is null
         or r.name             is distinct from lag(r.name) over w
         or r.cik              is distinct from lag(r.cik) over w
         or r.composite_figi   is distinct from lag(r.composite_figi) over w
         or r.share_class_figi is distinct from lag(r.share_class_figi) over w) as starts_run
    from reference r
    semi join mixed using (ticker)
    window w as (partition by ticker order by r.date)

), mixed_runs as (

    select
        ticker, name, cik, composite_figi, share_class_figi,
        min(date) as first_date,
        max(date) as last_date,
        count(*) as n_rows
    from (
        select
            *,
            sum(starts_run::int) over (partition by ticker order by date) as run
        from mixed_rows
    )
    group by ticker, run, name, cik, composite_figi, share_class_figi

), runs as (

    select g.*
    from groups g
    anti join mixed using (ticker)
    union all
    select * from mixed_runs

), named as (

    select
        *,
        -- The first word of the name. A new issuer changes it. A rename by the same
        -- fund family or company mostly keeps it.
        lower(regexp_extract(name, '[A-Za-z0-9]+')) as name_word
    from runs

), smoothed as (

    -- The vendor gives a name another FIGI for one session and the old one again on the
    -- next, as at the reverse splits of WORX, UPLD and BYND. A FIGI on one session
    -- between two sessions of the same other FIGI takes that FIGI, so the flicker does
    -- not start a security for one day. A run of more than one row has no flicker. The
    -- raw FIGIs stay in the run, because the join to the rows needs them.
    select
        * exclude (sc_before, sc_after, cf_before, cf_after),
        case when n_rows = 1 and share_class_figi <> sc_before and sc_before = sc_after
             then sc_before else share_class_figi end as sm_share_class_figi,
        case when n_rows = 1 and composite_figi <> cf_before and cf_before = cf_after
             then cf_before else composite_figi end as sm_composite_figi
    from (
        select
            *,
            last_value(share_class_figi ignore nulls) over earlier as sc_before,
            first_value(share_class_figi ignore nulls) over later  as sc_after,
            last_value(composite_figi ignore nulls) over earlier   as cf_before,
            first_value(composite_figi ignore nulls) over later    as cf_after
        from named
        window
            earlier as (partition by ticker order by first_date
                        rows between unbounded preceding and 1 preceding),
            later as (partition by ticker order by first_date
                      rows between 1 following and unbounded following)
    )

), flagged as (

    -- The vendor drops an identifier of a name on some dates, and it gives the CIK
    -- of a related issuer on others (CMSpC alternates between CMS Energy and
    -- Consumers Energy). An episode is a run of one ticker that stays with one
    -- security. A new episode starts when a FIGI changes, or when the first word of
    -- the name changes and the CIK is not the known one. A new issuer on a reused
    -- ticker can start with no CIK (STRC in July 2025).
    select
        *,
        coalesce(sm_share_class_figi <> last_value(sm_share_class_figi ignore nulls) over earlier, false)
        or coalesce(sm_composite_figi <> last_value(sm_composite_figi ignore nulls) over earlier, false)
        or coalesce(
            name_word <> last_value(name_word ignore nulls) over earlier
            and (cik is null
                 or cik <> coalesce(last_value(cik ignore nulls) over earlier, '')),
            false
        ) as starts_episode
    from smoothed
    window earlier as (
        partition by ticker order by first_date
        rows between unbounded preceding and 1 preceding
    )

), episodes as (

    select
        *,
        sum(starts_episode::int) over (
            partition by ticker order by first_date
            rows between unbounded preceding and current row
        ) as episode
    from flagged

), filled as (

    -- Each run takes the identifiers of its episode. The key is a label and not a
    -- price input, so a value that the vendor states on a later date of the same
    -- episode is not look-ahead.
    select
        * exclude (starts_episode),
        first_value(sm_share_class_figi ignore nulls) over whole as episode_share_class_figi,
        first_value(sm_composite_figi ignore nulls) over whole   as episode_composite_figi,
        first_value(cik ignore nulls) over whole                 as episode_cik
    from episodes
    window whole as (
        partition by ticker, episode order by first_date
        rows between unbounded preceding and unbounded following
    )

), need_sc as (

    -- The dates of a run that states no share class FIGI, in an episode that has one.
    select f.ticker, f.first_date, r.date, f.episode_share_class_figi as figi
    from filled f
    inner join reference r
        on  r.ticker = f.ticker
        and r.date between f.first_date and f.last_date
    where f.sm_share_class_figi is null and f.episode_share_class_figi is not null

), need_cf as (

    select f.ticker, f.first_date, r.date, f.episode_composite_figi as figi
    from filled f
    inner join reference r
        on  r.ticker = f.ticker
        and r.date between f.first_date and f.last_date
    where f.sm_composite_figi is null and f.episode_composite_figi is not null

), blocked_sc as (

    -- A filled FIGI that another ticker states on the same date belongs to that
    -- ticker. The old ticker of a renamed company (IIVI, now COHR) can pass to a new
    -- issuer with no FIGI, and the fill must not merge the two. This lists the blocked
    -- dates of each run.
    select ticker, first_date, list(date) as dates
    from (
        select n.ticker, n.first_date, n.date
        from need_sc n
        semi join reference r
            on  r.share_class_figi = n.figi
            and r.date             = n.date
            and r.ticker          <> n.ticker
        union
        select n.ticker, n.first_date, n.date
        from need_sc n
        semi join reference r
            on  r.composite_figi = n.figi
            and r.date           = n.date
            and r.ticker        <> n.ticker
    )
    group by ticker, first_date

), blocked_cf as (

    select ticker, first_date, list(date) as dates
    from (
        select n.ticker, n.first_date, n.date
        from need_cf n
        semi join reference r
            on  r.share_class_figi = n.figi
            and r.date             = n.date
            and r.ticker          <> n.ticker
        union
        select n.ticker, n.first_date, n.date
        from need_cf n
        semi join reference r
            on  r.composite_figi = n.figi
            and r.date           = n.date
            and r.ticker        <> n.ticker
    )
    group by ticker, first_date

), guarded_runs as (

    select
        f.*,
        s.dates as blocked_share_class_dates,
        c.dates as blocked_composite_dates
    from filled f
    left join blocked_sc s using (ticker, first_date)
    left join blocked_cf c using (ticker, first_date)

), guarded as (

    select
        r.ticker,
        r.date,
        r.name,
        r.type,
        r.primary_exchange,
        r.active,
        r.currency_name,
        r.cik,
        f.sm_composite_figi   as composite_figi,
        f.sm_share_class_figi as share_class_figi,
        f.name_word,
        f.episode,
        f.episode_cik,
        case when list_contains(f.blocked_share_class_dates, r.date) then null
             else f.episode_share_class_figi end as episode_share_class_figi,
        case when list_contains(f.blocked_composite_dates, r.date) then null
             else f.episode_composite_figi end   as episode_composite_figi
    from reference r
    inner join guarded_runs f
        on  f.ticker = r.ticker
        and f.name is not distinct from r.name
        and f.cik is not distinct from r.cik
        and f.composite_figi is not distinct from r.composite_figi
        and f.share_class_figi is not distinct from r.share_class_figi
        and r.date between f.first_date and f.last_date

)

select
    * exclude (episode_share_class_figi, episode_composite_figi),
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
    ) as episode_key,
    case
        when share_class_figi         is not null then 'share_class_figi'
        when episode_share_class_figi is not null then 'share_class_figi_filled'
        when coalesce(composite_figi, episode_composite_figi) is not null
            then 'composite_figi'
        when episode_cik              is not null then 'cik_ticker'
        else 'ticker_only'
    end as key_rule
from guarded

{#
  One row per change of FIGI key between two consecutive rows of a ticker, with each test
  of a link as a column. int_security_links takes the rows where linked is true.

  - The identity tests: the CIK stays (same_cik), and each name with no spaces or
    punctuation starts with the first word of the other name (same_name).
  - The structure tests: no gap in the bars (consecutive), the old key stops on every
    ticker (old_stops), the new key starts on the link date on every ticker (new_starts).
  - The price test (price_continues): the split-adjusted jump from the last old bar to
    the first new bar is within link_max_jump_sigma of the volatility of the old key.

  The rule links a change that passes every test. A reviewed decision in the seed
  security_link_decisions (link or refuse) overrides the identity tests only: a new
  holding company can take a new CIK (XOM, DMRC) or a new name (LH), and no name rule
  separates that from a different company on the ticker (GORO). The structure and price
  tests always apply. needs_review marks a change that passes the structure and price
  tests, fails an identity test and has no decision: it looks like one security.
#}

with episodes as (

    select
        ticker, date, name, name_word, cik, episode_cik, episode_key, key_rule,
        -- The name with no spaces and no punctuation.
        lower(regexp_replace(name, '[^A-Za-z0-9]', '', 'g')) as name_letters
    from {{ ref('int_ticker_episodes') }}

), changes as (

    -- A change from one FIGI key to another between two consecutive rows of a ticker.
    -- The first row of a ticker has no old key and drops out. The old CIK and name
    -- are the last known values, because the vendor can change the CIK inside an
    -- episode (CBLS).
    select *
    from (
        select
            ticker,
            date                                        as link_date,
            name,
            last_value(name ignore nulls) over earlier  as old_name,
            lag(episode_key) over w                     as old_key,
            episode_key                                 as new_key,
            lag(key_rule) over w                        as old_rule,
            key_rule                                    as new_rule,
            last_value(cik ignore nulls) over earlier   as old_cik,
            coalesce(cik, episode_cik)                  as new_cik,
            last_value(name_word ignore nulls) over earlier as old_word,
            name_word                                   as new_word,
            last_value(name_letters ignore nulls) over earlier as old_letters,
            name_letters                                as new_letters
        from episodes
        window
            w as (partition by ticker order by date),
            earlier as (
                partition by ticker order by date
                rows between unbounded preceding and 1 preceding
            )
    )
    where old_key <> new_key
      and old_rule not in ('cik_ticker', 'ticker_only')
      and new_rule not in ('cik_ticker', 'ticker_only')

), spans as (

    select episode_key, min(date) as first_date, max(date) as last_date
    from episodes
    group by episode_key

), sessions as (

    select date, row_number() over (order by date) as session
    from (select distinct date from {{ ref('stg_massive__day_aggs') }})

), splits as (

    -- Bounded at the last bar, as in int_prices_adjusted.
    select ticker, event_date, factor
    from {{ ref('int_corporate_actions') }}
    where kind = 'split'
      and event_date <= (select max(date) from sessions)

), bars as (

    -- The bars of the tickers that change key, with the session number, the key and the
    -- split-adjusted close. A later split with an unknown factor makes the close null.
    select
        b.ticker,
        b.date,
        s.session,
        e.episode_key,
        b.close * case when p.event_date is null then 1.0 else p.factor end as adj_close
    from (
        select ticker, date, close
        from {{ ref('stg_massive__day_aggs') }}
        where ticker in (select ticker from changes)
    ) b
    inner join sessions s on s.date = b.date
    inner join episodes e on e.ticker = b.ticker and e.date = b.date
    asof left join splits p
        on p.ticker = b.ticker
       and b.date   < p.event_date

), returns as (

    select
        *,
        ln(adj_close / lag(adj_close) over (partition by ticker, episode_key order by date)) as ret
    from bars

), sides as (

    -- The last bar of the old key before the change and the first bar of the new key.
    select
        c.ticker,
        c.link_date,
        max(b.date)    filter (where b.episode_key = c.old_key and b.date <  c.link_date) as last_old_bar,
        min(b.date)    filter (where b.episode_key = c.new_key and b.date >= c.link_date) as first_new_bar,
        max(b.session) filter (where b.episode_key = c.old_key and b.date <  c.link_date) as old_session,
        min(b.session) filter (where b.episode_key = c.new_key and b.date >= c.link_date) as new_session,
        ln(
            arg_min(b.adj_close, b.date) filter (where b.episode_key = c.new_key and b.date >= c.link_date)
            / arg_max(b.adj_close, b.date) filter (where b.episode_key = c.old_key and b.date < c.link_date)
        ) as jump
    from changes c
    left join bars b on b.ticker = c.ticker
    group by c.ticker, c.link_date

), risk as (

    -- The volatility of the old key: the last returns up to its last bar on the ticker.
    select ticker, link_date, stddev_samp(ret) as vol, count(ret) as n_returns
    from (
        select c.ticker, c.link_date, r.ret
        from changes c
        inner join returns r
            on r.ticker      = c.ticker
           and r.episode_key = c.old_key
           and r.date        < c.link_date
           and r.ret is not null
        qualify row_number() over (
            partition by c.ticker, c.link_date order by r.date desc
        ) <= {{ var('link_vol_window') }}
    )
    group by ticker, link_date

), tested as (

    -- A null fails each test.
    select
        c.ticker,
        c.link_date,
        c.old_name,
        c.name,
        c.old_cik,
        c.new_cik as cik,
        c.old_key,
        c.new_key,
        s.last_old_bar,
        s.first_new_bar,
        s.jump,
        r.vol,
        coalesce(c.old_cik = c.new_cik, false)                              as same_cik,
        -- A join of words passes (Exxon Mobil, ExxonMobil). A longer word fails (Gold
        -- Resource, Goldgroup).
        coalesce(starts_with(c.old_letters, c.new_word)
                 and starts_with(c.new_letters, c.old_word), false)         as same_name,
        coalesce(s.new_session = s.old_session + 1, false)                  as consecutive,
        -- A spin-off can keep the old FIGI on a new ticker.
        coalesce(o.last_date < c.link_date, false)                          as old_stops,
        -- A merger can give the ticker the FIGI of the other company.
        coalesce(n.first_date = c.link_date, false)                         as new_starts,
        -- A larger jump is the price of a different claim: cash in the deal, a
        -- distribution, or an exchange ratio that the vendor does not record.
        coalesce(r.n_returns >= {{ var('link_min_returns') }}
                 and abs(s.jump) <= {{ var('link_max_jump_sigma') }} * r.vol, false)
                                                                            as price_continues
    from changes c
    inner join sides s using (ticker, link_date)
    left join risk r using (ticker, link_date)
    inner join spans o on o.episode_key = c.old_key
    inner join spans n on n.episode_key = c.new_key

)

select
    t.*,
    d.decision,
    d.reason,
    t.consecutive and t.old_stops and t.new_starts and t.price_continues
        and case d.decision when 'link' then true
                            when 'refuse' then false
                            else t.same_cik and t.same_name end            as linked,
    t.consecutive and t.old_stops and t.new_starts and t.price_continues
        and not (t.same_cik and t.same_name) and d.decision is null        as needs_review
from tested t
left join {{ ref('security_link_decisions') }} d using (ticker, link_date)

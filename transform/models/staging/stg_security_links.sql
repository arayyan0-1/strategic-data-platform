{#
  One row per link: a new FIGI on a ticker that continues the security of the old
  FIGI, as in a reorganization into a new holding company. stg_tickers gives the rows
  of the new key the first key of the chain. A refused link starts a new security.
#}

with recursive episodes as (

    select ticker, date, name, name_word, cik, episode_cik, episode_key, key_rule
    from {{ ref('stg_ticker_episodes') }}

), changes as (

    -- A change from one FIGI key to another between two consecutive rows of a ticker.
    -- The first row of a ticker has no old key and drops out. The old CIK and name
    -- word are the last known values, because the vendor can change the CIK inside an
    -- episode (CBLS).
    select *
    from (
        select
            ticker,
            date                                        as link_date,
            name,
            lag(episode_key) over w                     as old_key,
            episode_key                                 as new_key,
            lag(key_rule) over w                        as old_rule,
            key_rule                                    as new_rule,
            last_value(cik ignore nulls) over earlier   as old_cik,
            coalesce(cik, episode_cik)                  as new_cik,
            last_value(name_word ignore nulls) over earlier as old_word,
            name_word                                   as new_word
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
    from (select distinct cast(date as date) as date from {{ lake('us_stocks_day_aggs', 'date') }})

), splits as (

    -- Bounded at the last bar, as in stg_prices_adjusted.
    select ticker, event_date, factor
    from {{ ref('stg_corporate_actions') }}
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
        select ticker, cast(date as date) as date, close
        from {{ lake('us_stocks_day_aggs', 'date') }}
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

), candidates as (

    -- A null fails each condition.
    select
        c.ticker,
        c.link_date,
        c.name,
        c.new_cik as cik,
        c.old_key,
        c.new_key,
        s.last_old_bar,
        s.first_new_bar,
        s.jump,
        r.vol
    from changes c
    inner join sides s using (ticker, link_date)
    left join risk r using (ticker, link_date)
    inner join spans o on o.episode_key = c.old_key
    inner join spans n on n.episode_key = c.new_key
    where coalesce(c.old_cik = c.new_cik, false)
      and coalesce(c.old_word = c.new_word, false)
      -- No gap in the bars of the ticker.
      and coalesce(s.new_session = s.old_session + 1, false)
      -- The old key stops on every ticker. A spin-off can keep the old FIGI on a new ticker.
      and o.last_date < c.link_date
      -- The new key starts here on every ticker. A merger can give the ticker the FIGI
      -- of the other company.
      and n.first_date = c.link_date
      -- The price continues. A larger jump is the price of a different claim: cash in
      -- the deal, a distribution, or an exchange ratio that the vendor does not record.
      and coalesce(
          r.n_returns >= {{ var('link_min_returns') }}
          and abs(s.jump) <= {{ var('link_max_jump_sigma') }} * r.vol,
          false
      )

), links as (

    -- One link for each old key and each new key.
    select *
    from candidates
    qualify count(*) over (partition by old_key) = 1
        and count(*) over (partition by new_key) = 1

), chains as (

    -- Follow a run of links (A to B to C) back to the first key. Each step goes to an
    -- earlier link, so the recursion stops. A cycle has no first key and drops out.
    select new_key, old_key as root_key, link_date as step_date
    from links
    union all
    select c.new_key, l.old_key, l.link_date
    from chains c
    inner join links l
        on l.new_key    = c.root_key
       and l.link_date  < c.step_date

)

select l.*, c.root_key
from links l
inner join chains c using (new_key)
where c.root_key not in (select new_key from links)

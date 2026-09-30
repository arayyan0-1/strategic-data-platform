{#
  The cross-asset board: one row per ETF of the monitor_etfs seed. Each ETF is the
  security that holds its ticker on the last session, so a ticker that a new fund
  reused (IBIT) shows only the current fund. Returns use the total-adjusted close and
  count sessions back from the last one: a week is 5, a month 21, a quarter 63, a
  year 252. The year to date starts at the last close of the previous year. z_1d, z_1w
  and z_1m are the scores of the log returns over 1, 5 and 21 sessions (market_scores):
  a score of 3 is as rare as a 3 sigma move of a normal distribution on the history of
  the monitor ETFs. They are null for an ETF with a 20-session volatility under the var
  monitor_min_etf_vol a year.
#}

with last_session as (

    select max(date) as d from {{ ref('int_prices_adjusted') }}

), keys as (

    select e.ticker, e.asset_group, e.label, e.position, u.security_key, u.name
    from {{ ref('monitor_etfs') }} e
    join {{ ref('int_universe') }} u
        on u.ticker = e.ticker and u.date = (select d from last_session)

), series as (

    select
        k.ticker, k.asset_group, k.label, k.position, k.name,
        p.date, p.close, p.volume,
        coalesce(p.adj_close_total, p.adj_close_split) as px
    from keys k
    join {{ ref('int_universe') }} u
        on u.security_key = k.security_key and u.is_primary_line
    join {{ ref('int_prices_adjusted') }} p
        on p.ticker = u.ticker and p.date = u.date

), ranked as (

    select
        *,
        row_number() over w_desc                        as back,
        ln(px / lag(px) over w_asc)                     as log_ret,
        year(max(date) over (partition by ticker))      as last_year
    from series
    window w_desc as (partition by ticker order by date desc),
           w_asc  as (partition by ticker order by date)

), board as (

    select
        ticker,
        any_value(asset_group)                                              as asset_group,
        any_value(label)                                                    as label,
        any_value(position)                                                 as position,
        any_value(name)                                                     as name,
        max(date)                                                           as last_date,
        max(close) filter (where back = 1)                                  as close,
        max(px) filter (where back = 1) / max(px) filter (where back = 2) - 1   as ret_1d,
        max(px) filter (where back = 1) / max(px) filter (where back = 6) - 1   as ret_1w,
        max(px) filter (where back = 1) / max(px) filter (where back = 22) - 1  as ret_1m,
        max(px) filter (where back = 1) / max(px) filter (where back = 64) - 1  as ret_3m,
        max(px) filter (where back = 1) / max(px) filter (where back = 127) - 1 as ret_6m,
        max(px) filter (where back = 1)
            / arg_max(px, date) filter (where year(date) < last_year) - 1   as ret_ytd,
        max(px) filter (where back = 1) / max(px) filter (where back = 253) - 1 as ret_1y,
        max(px) filter (where back = 1) / max(px) filter (where back <= 252) - 1 as off_52w_high,
        max(px) filter (where back = 1) / min(px) filter (where back <= 252) - 1 as over_52w_low,
        stddev_samp(log_ret) filter (where back <= 20) * sqrt(252)          as vol_20,
        max(volume) filter (where back = 1)
            / nullif(avg(volume) filter (where back between 2 and 21), 0)   as rel_volume
    from ranked
    group by ticker

), scores as (

    select
        subject                          as ticker,
        max(z) filter (where h = 1)     as z_1d,
        max(z) filter (where h = 5)     as z_1w,
        max(z) filter (where h = 21)    as z_1m
    from {{ ref('market_scores') }}
    where family = 'etf' and date = (select d from last_session)
    group by subject

)

select b.*, s.z_1d, s.z_1w, s.z_1m
from board b
left join scores s using (ticker)

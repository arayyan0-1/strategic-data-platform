-- A change of ticker must not restart the windows of a security. A security in the
-- universe on the session before a change passes the history and seasoning checks on
-- the session of the change. FB became META on 2022-06-09 and PSTG became P on
-- 2026-04-17. Each stays in the universe, with the count of the day before plus one and
-- an average that has moved by one session only.
with series as (
    select
        security_key,
        ticker,
        date,
        in_universe,
        passes_history,
        passes_seasoning,
        adv,
        bars_seen,
        lag(ticker) over w      as prev_ticker,
        lag(in_universe) over w as prev_in_universe,
        lag(adv) over w         as prev_adv,
        lag(bars_seen) over w   as prev_bars_seen
    from {{ ref('int_universe') }}
    -- A change needs two tickers, so the windows skip a security with one ticker.
    where is_primary_line
      and security_key in (
        select security_key from {{ ref('int_universe') }}
        where is_primary_line
        group by security_key
        having count(distinct ticker) > 1
      )
    window w as (partition by security_key order by date)
), changes as (
    select * from series where prev_ticker <> ticker
), named (ticker, date) as (
    values ('META', date '2022-06-09'), ('P', date '2026-04-17')
)
select 'a change loses the windows' as failure, security_key, ticker, date
from changes
where prev_in_universe and not (passes_history and passes_seasoning)
union all
select 'a named change does not continue', c.security_key, n.ticker, n.date
from named n
left join changes c
    on c.ticker = n.ticker
   and c.date   = n.date
where c.security_key is null
   or not (c.prev_in_universe and c.in_universe)
   or c.bars_seen is distinct from c.prev_bars_seen + 1
   or not coalesce(c.adv between 0.8 * c.prev_adv and 1.25 * c.prev_adv, false)

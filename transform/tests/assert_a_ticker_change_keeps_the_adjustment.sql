-- In the series of a security, a change of ticker must not move the factors, unless
-- an event of either ticker lies between the two bars.
with series as (
    select u.security_key, p.ticker, p.date, p.split_factor, p.dividend_factor
    from {{ ref('int_prices_adjusted') }} p
    inner join {{ ref('int_universe') }} u
        on u.ticker = p.ticker and u.date = p.date
    -- A change needs two tickers, so the windows skip a security with one ticker.
    where u.is_primary_line
      and u.security_key in (
        select security_key from {{ ref('int_universe') }}
        where is_primary_line
        group by security_key
        having count(distinct ticker) > 1
      )
), steps as (
    select
        *,
        lag(ticker)          over w as prev_ticker,
        lag(date)            over w as prev_date,
        lag(split_factor)    over w as prev_split,
        lag(dividend_factor) over w as prev_dividend
    from series
    window w as (partition by security_key order by date)
), changes as (
    select
        s.*,
        exists (
            select 1 from {{ ref('int_corporate_actions') }} a
            where a.kind = 'split'
              and a.ticker in (s.ticker, s.prev_ticker)
              and a.event_date > s.prev_date and a.event_date <= s.date
        ) as split_between,
        exists (
            select 1 from {{ ref('int_corporate_actions') }} a
            where a.kind = 'dividend'
              and a.ticker in (s.ticker, s.prev_ticker)
              and a.event_date > s.prev_date and a.event_date <= s.date
        ) as dividend_between
    from steps s
    where s.prev_ticker <> s.ticker
)
select *
from changes
where (not split_between and abs(ln(split_factor / prev_split)) > 1e-9)
   or (not dividend_between and abs(ln(dividend_factor / prev_dividend)) > 1e-9)

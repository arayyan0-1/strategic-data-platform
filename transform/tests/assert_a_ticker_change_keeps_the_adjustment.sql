-- In the series of a security, a change of ticker must not move the factors, unless
-- an event of either ticker lies between the two bars.
with series as (
    select u.security_key, p.ticker, p.date, p.split_factor, p.dividend_factor
    from {{ ref('stg_prices_adjusted') }} p
    inner join {{ ref('stg_universe') }} u
        on u.ticker = p.ticker and u.date = p.date
    where u.is_primary_line
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
            select 1 from {{ ref('stg_corporate_actions') }} a
            where a.kind = 'split'
              and a.ticker in (s.ticker, s.prev_ticker)
              and a.event_date > s.prev_date and a.event_date <= s.date
        ) as split_between,
        exists (
            select 1 from {{ ref('stg_corporate_actions') }} a
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

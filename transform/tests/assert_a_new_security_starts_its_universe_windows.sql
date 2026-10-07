{# A returned row is a primary-line count that is not the session's rank, or a new security on a
   used ticker (FB, WOLF) whose windows do not start at zero. #}
with recomputed as (
    select
        ticker,
        date,
        row_number() over (partition by security_key order by date) as bars_seen
    from {{ ref('int_security_lines') }}
    where is_primary_line
), named (ticker, date) as (
    values ('FB', date '2025-06-26'), ('WOLF', date '2025-09-29')
), checked as (
    -- A count over a join, not a correlated NOT EXISTS. DuckDB 1.5 returns a wrong result
    -- for a correlated subquery with a range condition and a <> condition.
    select n.ticker, n.date, u.bars_seen, u.days_in_window, count(o.ticker) as n_other_bars
    from named n
    left join {{ ref('int_universe') }} u
        on u.ticker = n.ticker
       and u.date   = n.date
    left join {{ ref('int_security_lines') }} o
        on o.ticker       = n.ticker
       and o.date         < n.date
       and o.security_key <> u.security_key
    group by all
)
select 'the count is not the rank of the session' as failure, u.ticker, u.date, u.bars_seen
from {{ ref('int_universe') }} u
inner join recomputed r
    on u.ticker = r.ticker
   and u.date   = r.date
where u.is_primary_line
  and u.bars_seen is distinct from r.bars_seen
union all
select 'a new security on a used ticker does not start at one', ticker, date, bars_seen
from checked
where bars_seen is distinct from 1
   or days_in_window is distinct from 0
   or n_other_bars = 0

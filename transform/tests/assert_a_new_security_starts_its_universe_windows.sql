-- A new security starts its windows at zero, also when it takes a ticker that another
-- security used. FB was Facebook until 2022 and a fund from 2025-06-26. WOLF came out of
-- bankruptcy as a new security on 2025-09-29. The count of sessions of a security is
-- the rank of the session in the series of its primary line.
with recomputed as (
    select
        ticker,
        date,
        row_number() over (partition by security_key order by date) as bars_seen
    from {{ ref('int_security_lines') }}
    where is_primary_line
), named (ticker, date) as (
    values ('FB', date '2025-06-26'), ('WOLF', date '2025-09-29')
)
select 'the count is not the rank of the session' as failure, u.ticker, u.date, u.bars_seen
from {{ ref('int_universe') }} u
inner join recomputed r
    on u.ticker = r.ticker
   and u.date   = r.date
where u.is_primary_line
  and u.bars_seen is distinct from r.bars_seen
union all
select 'a new security on a used ticker does not start at one', n.ticker, n.date, u.bars_seen
from named n
left join {{ ref('int_universe') }} u
    on u.ticker = n.ticker
   and u.date   = n.date
where u.bars_seen is distinct from 1
   or u.days_in_window is distinct from 0
   or not exists (
        select 1
        from {{ ref('int_security_lines') }} o
        where o.ticker       = n.ticker
          and o.date         < n.date
          and o.security_key <> u.security_key
   )

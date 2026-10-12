{# A returned row is a month of region:developed or us:small where r3, r6, vol12 or dd12 differs
   by over 1e-9 from the same feature computed over the rows before it (3, 6 and 12 rows), or a
   month that only one of the two lists. The months of these two indexes are consecutive, so
   rows and calendar months agree. #}
with r as (

    select index_key, month, ret, row_number() over (partition by index_key order by month) as n
    from {{ ref('int_index_returns') }}
    where index_key in ('region:developed', 'us:small')

), wealth as (

    select
        a.index_key,
        a.month,
        b.n,
        exp(sum(ln(1 + b.ret)) over (partition by a.index_key, a.month order by b.n))   as w
    from r a
    join r b on b.index_key = a.index_key and b.n between a.n - 11 and a.n

), drawdown as (

    select
        index_key,
        month,
        min(w / greatest(1.0, peak)) - 1                                                 as dd12
    from (
        select *, max(w) over (partition by index_key, month order by n) as peak
        from wealth
    )
    group by 1, 2

), windows as (

    select
        a.index_key,
        a.month,
        exp(sum(ln(1 + b.ret)) filter (where b.n > a.n - 3)) - 1                         as r3,
        exp(sum(ln(1 + b.ret)) filter (where b.n > a.n - 6)) - 1                         as r6,
        stddev_samp(b.ret)                                                               as vol12
    from r a
    join r b on b.index_key = a.index_key and b.n between a.n - 11 and a.n
    group by 1, 2
    having count(*) = 12

), second as (

    select w.index_key, w.month, w.r3, w.r6, w.vol12, d.dd12
    from windows w
    join drawdown d using (index_key, month)

)

select coalesce(s.index_key, f.index_key) as index_key, coalesce(s.month, f.month) as month,
       f.r3, s.r3 as second_r3, f.r6, s.r6 as second_r6, f.vol12, s.vol12 as second_vol12,
       f.dd12, s.dd12 as second_dd12
from second s
full join (
    select * from {{ ref('fund_index_features') }}
    where index_key in ('region:developed', 'us:small')
) f using (index_key, month)
where s.month is null or f.month is null
   or abs(f.r3 - s.r3) > 1e-9 or abs(f.r6 - s.r6) > 1e-9
   or abs(f.vol12 - s.vol12) > 1e-9 or abs(f.dd12 - s.dd12) > 1e-9

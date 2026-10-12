{# A returned row is a month of us:market where mom, val or trend differs by over 1e-9 from the
   same signal computed over the rows before it (11, 48 and 12 rows), or a month that only one of
   the two lists. us:market has no missing month, so rows and calendar months agree. #}
with r as (

    select month, ret, rf, row_number() over (order by month) as n
    from {{ ref('int_index_returns') }}
    where index_key = 'us:market'

), mom as (

    select a.month, exp(sum(ln(1 + b.ret))) - 1 as mom
    from r a join r b on b.n between a.n - 11 and a.n - 1
    group by a.month
    having count(*) = 11

), val as (

    select a.month, 1 - exp(sum(ln(1 + b.ret))) as val
    from r a join r b on b.n between a.n - 59 and a.n - 12
    group by a.month
    having count(*) = 48

), trend as (

    select a.month, exp(sum(ln(1 + b.ret))) - exp(sum(ln(1 + b.rf))) as trend
    from r a join r b on b.n between a.n - 11 and a.n
    group by a.month
    having count(*) = 12

), second as (

    select t.month, m.mom, v.val, t.trend
    from trend t
    left join mom m using (month)
    left join val v using (month)

)

select coalesce(s.month, h.month) as month, h.mom, s.mom as second_mom, h.val, s.val as second_val,
       h.trend, s.trend as second_trend
from second s
full join (select * from {{ ref('fund_signal_history') }} where index_key = 'us:market') h
    using (month)
where s.month is null or h.month is null
   or (h.mom is null) <> (s.mom is null) or abs(h.mom - s.mom) > 1e-9
   or (h.val is null) <> (s.val is null) or abs(h.val - s.val) > 1e-9
   or (h.trend is null) <> (s.trend is null) or abs(h.trend - s.trend) > 1e-9

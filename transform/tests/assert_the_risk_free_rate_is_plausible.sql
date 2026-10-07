{# A returned row is a session where rf is null (after the first), negative or over 10 bps, or a
   month where it differs from the Fama-French rf by over 20 bps. The library rounds its daily rf
   to 0.01%, so a month can differ by about 11 bps from rounding alone. #}
with ours as (
    select date_trunc('month', date) as m, sum(rf) as rf, count(*) as n
    from {{ ref('rates') }}
    group by 1
), ff as (
    select date_trunc('month', date) as m, sum(rf) as rf, count(*) as n
    from {{ ref('stg_french__factors') }}
    group by 1
)
select 'range' as check_name, date as m, rf, null as ff_rf
from {{ ref('rates') }}
where (rf is null and date > (select min(date) from {{ ref('rates') }}))
   or rf < 0 or rf > 0.001
union all
select 'against the library', o.m, o.rf, f.rf
from ours o join ff f using (m)
where o.n = f.n and abs(o.rf - f.rf) > 0.002

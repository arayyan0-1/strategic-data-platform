-- One row per security and session. A fan-out would double count a name.
select security_key, date, count(*) as n
from {{ ref('mart_signals') }}
group by security_key, date
having count(*) > 1

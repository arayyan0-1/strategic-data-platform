{# A returned row is a duplicate (security_key, date): a fan-out would double count a name. #}
select security_key, date, count(*) as n
from {{ ref('signals') }}
group by security_key, date
having count(*) > 1

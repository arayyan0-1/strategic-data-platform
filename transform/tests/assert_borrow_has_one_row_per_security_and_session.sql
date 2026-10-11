{# A returned row is a duplicate (date, security_key): two symbols mapped to one security. #}
select date, security_key, count(*) as n
from {{ ref('borrow') }}
group by 1, 2
having count(*) > 1

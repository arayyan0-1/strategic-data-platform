{# A returned row is a duplicate (security_key, date): two candidates share a security. #}
select security_key, date, count(*) as n
from {{ ref('fund_universe') }}
group by 1, 2
having count(*) > 1

{# A returned row is a duplicate (date, family, factor): it would count a return twice. #}
select date, family, factor, count(*) as n
from {{ ref('factor_returns') }}
group by all
having count(*) > 1

{# A returned row is a duplicate (index_key, month): two inputs gave one index the same month. #}
select index_key, month, count(*) as n
from {{ ref('int_index_returns') }}
group by 1, 2
having count(*) > 1

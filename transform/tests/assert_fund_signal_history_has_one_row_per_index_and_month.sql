{# A returned row is a duplicate (index_key, month): an index sits in two universes, or a month
   repeats. #}
select index_key, month, count(*) as n
from {{ ref('fund_signal_history') }}
group by 1, 2
having count(*) > 1

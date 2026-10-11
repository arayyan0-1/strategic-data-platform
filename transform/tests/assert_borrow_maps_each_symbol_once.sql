{# A returned row is a duplicate (date, symbol) in int_borrow: a key join fanned a symbol out. #}
select date, symbol, count(*) as n
from {{ ref('int_borrow') }}
group by 1, 2
having count(*) > 1

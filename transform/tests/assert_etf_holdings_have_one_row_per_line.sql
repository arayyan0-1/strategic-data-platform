{# A returned row is a duplicate (fund, date, position): the as-of join fanned a line out. #}
select fund, date, position, count(*) as n
from {{ ref('int_etf_holdings') }}
group by 1, 2, 3
having count(*) > 1

{# A returned row is a duplicate (fund, date): the history and the screener both kept a day. #}
select fund, date, count(*) as n
from {{ ref('etf_daily') }}
group by 1, 2
having count(*) > 1

{# A returned row is a duplicate (ticker, date): a join turned one price into two. #}
select ticker, date, count(*) as n
from {{ ref('int_prices_adjusted') }}
group by 1, 2
having count(*) > 1

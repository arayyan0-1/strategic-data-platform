{# A returned row is a fund and report date whose weights sum to more than 1: a line counted twice,
   or a weight taken over a total that leaves lines out. #}
select fund, report_date, sum(weight) as total_weight
from {{ ref('int_fund_holdings_history') }}
group by 1, 2
having sum(weight) > 1 + 1e-9

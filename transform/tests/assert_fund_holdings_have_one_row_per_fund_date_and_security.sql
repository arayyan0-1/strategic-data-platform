{# A returned row is a duplicate (fund, report_date, security_key): lines of one security were not
   summed, or a key join fanned a line out. #}
select fund, report_date, security_key, count(*) as n
from {{ ref('int_fund_holdings_history') }}
group by 1, 2, 3
having count(*) > 1

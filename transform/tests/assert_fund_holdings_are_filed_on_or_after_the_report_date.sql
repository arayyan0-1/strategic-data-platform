{# A returned row was public before the date it describes, so a study would read it too early. #}
select fund, report_date, filed
from {{ ref('int_fund_holdings_history') }}
where filed < report_date

{# A returned row is a seed fund whose fee is null or outside 0.01 to 1.5, so not a plausible percent a year
   (a fraction such as 0.0003 or a count of basis points would fall outside). #}
select fund, expense_ratio
from {{ ref('fund_extra_etfs') }}
where expense_ratio is null or expense_ratio < 0.01 or expense_ratio > 1.5

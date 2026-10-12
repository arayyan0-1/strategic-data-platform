{# A returned row means no fund of the cash role is a candidate. #}
select count(*) as n_cash_candidates
from {{ ref('fund_candidates') }}
where is_candidate and role = 'cash'
having count(*) = 0

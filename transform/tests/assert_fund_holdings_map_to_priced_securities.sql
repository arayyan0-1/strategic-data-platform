{# A returned row is a holding on a security_key that core.securities lacks, so it has no prices. #}
select h.fund, h.report_date, h.security_key
from {{ ref('int_fund_holdings_history') }} h
left join {{ ref('securities') }} s using (security_key)
where s.security_key is null

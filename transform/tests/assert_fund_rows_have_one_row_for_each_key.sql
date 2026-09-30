-- One fit for each fund and first session, and one residual for each fund and session. A
-- second row would count a fund return twice.
select 'fund_exposures' as model, security_key, fit_date as date, count(*) as n
from {{ ref('fund_exposures') }}
group by all
having count(*) > 1

union all

select 'fund_residuals', security_key, date, count(*)
from {{ ref('fund_residuals') }}
group by all
having count(*) > 1

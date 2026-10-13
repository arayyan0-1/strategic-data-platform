{# A returned row is a fund of fund_candidates or fund_universe whose role is null or not one of equity,
   bond, gold, commodity, bitcoin and cash. #}
select 'fund_candidates' as model, fund
from {{ ref('fund_candidates') }}
where role is null or role not in ('equity', 'bond', 'gold', 'commodity', 'bitcoin', 'cash')

union all

select 'fund_universe', fund
from {{ ref('fund_universe') }}
where role is null or role not in ('equity', 'bond', 'gold', 'commodity', 'bitcoin', 'cash')

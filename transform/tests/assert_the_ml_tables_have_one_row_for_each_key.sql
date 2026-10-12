{# A returned row is a duplicate key in a table of the machine-learning study. #}
select 'fund_index_features' as model, index_key as name, month, cast(null as varchar) as variant,
       count(*) as n
from {{ ref('fund_index_features') }}
group by all
having count(*) > 1

union all

select 'fund_features', fund, month, cast(null as varchar), count(*)
from {{ ref('fund_features') }}
group by all
having count(*) > 1

union all

select 'fund_ml_ic', universe, cast(null as date), model, count(*)
from {{ ref('fund_ml_ic') }}
group by all
having count(*) > 1

union all

select 'fund_ml_scores', fund, month, model, count(*)
from {{ ref('fund_ml_scores') }}
group by all
having count(*) > 1

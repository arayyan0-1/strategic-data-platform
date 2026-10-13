{# A returned row is a duplicate (index_key, period) of fund_asset_gate, or an asset of
   fund_asset_history that lacks a development or an all row. #}
with periods (period) as (

    values ('development'), ('all')

), expected as (

    select k.index_key, p.period
    from (select distinct index_key from {{ ref('fund_asset_history') }}) k
    cross join periods p

), counted as (

    select index_key, period, count(*) as n
    from {{ ref('fund_asset_gate') }}
    group by 1, 2

)

select e.index_key, e.period, coalesce(c.n, 0) as n
from expected e
left join counted c using (index_key, period)
where coalesce(c.n, 0) <> 1
union all
select index_key, period, n
from counted
where n > 1 or (index_key, period) not in (select index_key, period from expected)

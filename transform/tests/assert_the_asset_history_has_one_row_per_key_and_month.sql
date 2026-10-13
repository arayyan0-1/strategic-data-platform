{# A returned row is a duplicate (index_key, month) of fund_asset_history, or one of the ten assets
   with no row at all. #}
with assets (index_key) as (

    values ('region:developed'), ('bond:ust_2y'), ('bond:ust_5y'), ('bond:ust_10y'),
           ('bond:ust_30y'), ('bond:tips_10y'), ('bond:corp_baa'), ('gold'), ('commodity:all'),
           ('crypto:bitcoin')

), counted as (

    select index_key, month, count(*) as n
    from {{ ref('fund_asset_history') }}
    group by 1, 2

)

select index_key, month, n, 'duplicate' as problem
from counted
where n > 1
union all
select a.index_key, null, 0, 'no rows'
from assets a
where not exists (select 1 from counted c where c.index_key = a.index_key)

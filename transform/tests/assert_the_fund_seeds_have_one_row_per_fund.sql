{# A returned row is a null fund, or a fund with more than one row, in a fund seed. #}
select 'fund_extra_etfs' as seed, fund, count(*) as n
from {{ ref('fund_extra_etfs') }}
group by fund
having fund is null or count(*) > 1

union all

select 'fund_index_map', fund, count(*)
from {{ ref('fund_index_map') }}
group by fund
having fund is null or count(*) > 1

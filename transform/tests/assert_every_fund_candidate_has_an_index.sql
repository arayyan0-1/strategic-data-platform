{# A returned row is a candidate with no row in fund_index_map. Warns only: a new screener fund must not
   stop the build, and it has no index until someone reviews it. #}
{{ config(severity='warn') }}
select c.fund, c.name, c.role
from {{ ref('fund_candidates') }} c
left join {{ ref('fund_index_map') }} m on m.fund = c.fund
where c.is_candidate and m.fund is null

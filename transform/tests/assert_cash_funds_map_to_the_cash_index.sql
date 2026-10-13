{# A returned row is a candidate whose role and index disagree: a cash fund without cash:tbill, or a fund
   of any other role with it. A candidate with no map row is left to
   assert_every_fund_candidate_has_an_index. #}
select c.fund, c.role, m.index_key
from {{ ref('fund_candidates') }} c
inner join {{ ref('fund_index_map') }} m on m.fund = c.fund
where c.is_candidate
  and (c.role = 'cash') is distinct from (m.index_key is not distinct from 'cash:tbill')

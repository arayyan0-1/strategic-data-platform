{# A returned row is a candidate whose non-empty index key is outside the family of its role: bond maps to
   bond:*, gold to gold, commodity to commodity:*, bitcoin to crypto:bitcoin, cash to cash:tbill, and equity
   to region:*, country:* or us:*. A candidate with no map row or an empty key is left to
   assert_every_fund_candidate_has_an_index. #}
select c.fund, c.role, m.index_key
from {{ ref('fund_candidates') }} c
inner join {{ ref('fund_index_map') }} m on m.fund = c.fund
where c.is_candidate
  and nullif(trim(m.index_key), '') is not null
  and not coalesce(
      case c.role
          when 'bond'      then m.index_key like 'bond:%'
          when 'gold'      then m.index_key = 'gold'
          when 'commodity' then m.index_key like 'commodity:%'
          when 'bitcoin'   then m.index_key = 'crypto:bitcoin'
          when 'cash'      then m.index_key = 'cash:tbill'
          when 'equity'    then m.index_key like 'region:%' or m.index_key like 'country:%'
                                or m.index_key like 'us:%'
      end, false)

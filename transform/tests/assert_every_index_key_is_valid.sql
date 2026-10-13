{# A returned row is a map row whose non-empty index_key is not a region, a country of the French
   international countries file, a US series, cash:tbill, a bond, gold, commodity or bitcoin series. An
   empty key is allowed. #}
select fund, index_key
from {{ ref('fund_index_map') }}
where nullif(trim(index_key), '') is not null
  and not (
    regexp_full_match(index_key, 'region:(developed|developed_ex_us|north_america|europe|japan'
                                 || '|asia_pacific_ex_japan|emerging)')
    or regexp_full_match(index_key, 'us:(market|small|big|value|growth|momentum|quality|low_vol)')
    or index_key = 'cash:tbill'
    or regexp_full_match(index_key, 'country:(australia|austria|belgium|canada|denmark|finland'
                                    || '|france|germany|hong_kong|ireland|italy|japan|netherlands'
                                    || '|new_zealand|norway|singapore|spain|sweden|switzerland'
                                    || '|united_kingdom)')
    or regexp_full_match(index_key, 'bond:(ust_2y|ust_5y|ust_10y|ust_30y|tips_10y|corp_aaa|corp_baa)')
    or index_key = 'gold'
    or regexp_full_match(index_key, 'commodity:(all|energy|metals|agriculture)')
    or index_key = 'crypto:bitcoin'
  )

{# A returned row is a map row whose non-empty index_key is not a region, a country of the French
   international countries file, a US series or cash:tbill. An empty key is allowed. #}
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
  )

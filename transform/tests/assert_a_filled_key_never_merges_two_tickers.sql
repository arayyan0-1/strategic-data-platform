{# A returned row is a session where two tickers share a key through a filled FIGI. Only a
   self-stated FIGI (a when-issued line) may be shared. #}
select security_key, date, list(ticker) as tickers, list(key_rule) as rules
from {{ ref('int_universe') }}
group by security_key, date
having count(*) > 1 and bool_or(key_rule = 'share_class_figi_filled')

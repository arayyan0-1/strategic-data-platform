-- Two tickers share a key on one session only when both state that FIGI themselves (a
-- when-issued line). A filled FIGI that joins a second ticker merges two securities.
select security_key, date, list(ticker) as tickers, list(key_rule) as rules
from {{ ref('stg_universe') }}
group by security_key, date
having count(*) > 1 and bool_or(key_rule = 'share_class_figi_filled')

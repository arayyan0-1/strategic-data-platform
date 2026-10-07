{# A returned row is a price with a later dividend of unknown (null) factor but a non-null
   adj_close_total. Unknown is not 1.0. #}
select ticker, date, next_dividend_date, dividend_factor, adj_close_total
from {{ ref('int_prices_adjusted') }}
where next_dividend_date is not null
  and dividend_factor is null
  and adj_close_total is not null

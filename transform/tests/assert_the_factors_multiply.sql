{# A returned row is a price where total_factor is not split_factor x dividend_factor, or
   adj_close_total is not close x total_factor. #}
select ticker, date, split_factor, dividend_factor, total_factor
from {{ ref('int_prices_adjusted') }}
where total_factor is distinct from (split_factor * dividend_factor)
   or adj_close_total is distinct from (close * split_factor * dividend_factor)

{# A returned row is a price whose next_split_date is on or before its date. A split applies
   overnight, so the join must be strict. #}
select ticker, date, next_split_date
from {{ ref('int_prices_adjusted') }}
where next_split_date is not null
  and next_split_date <= date

{# The named check: AAPL's 2-for-1 split on 2005-02-28 has factor 1/56 (1/2 x 1/7 x 1/4 with the
   later splits). A returned row means the cumulative semantics are wrong. #}
select *
from {{ ref('int_corporate_actions') }}
where ticker = 'AAPL'
  and kind = 'split'
  and event_date = date '2005-02-28'
  and (abs(factor - 0.017857) > 5e-7 or abs(factor_chained_check - 1.0/56.0) > 1e-9)

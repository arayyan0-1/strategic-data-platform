-- The named check. The 2-for-1 split of AAPL on 2005-02-28 has factor 0.017857,
-- which is 1/56, that is 1/2 x 1/7 x 1/4 compounded with the splits of 2014 and
-- of 2020. If this number does not appear, the cumulative semantics are wrong.
select *
from {{ ref('stg_corporate_actions') }}
where ticker = 'AAPL'
  and kind = 'split'
  and event_date = date '2005-02-28'
  and (abs(factor - 0.017857) > 5e-7 or abs(factor_chained_check - 1.0/56.0) > 1e-9)

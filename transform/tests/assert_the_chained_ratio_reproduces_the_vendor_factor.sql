-- The vendor factor is cumulative and it includes the event itself. The product
-- of split_from/split_to over every event at or after this one must reproduce it.
-- The vendor rounds the factor to six decimal places.
-- A collapsed event has more than one ratio for one date. The factor is
-- cumulative, so that ambiguity travels backwards through every earlier event of
-- the same ticker. Those events are out of scope. The exposure is measurable in
-- the column has_collapsed_at_or_after.
select ticker, event_date, factor, factor_chained_check,
       abs(factor - factor_chained_check) as diff
from {{ ref('stg_corporate_actions') }}
where kind = 'split'
  and not has_collapsed_at_or_after
  and abs(factor - factor_chained_check) > 1e-6 * greatest(factor, 1.0)

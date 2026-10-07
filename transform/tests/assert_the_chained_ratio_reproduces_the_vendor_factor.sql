{# A returned row is a split whose chained split_from/split_to product, over every event at or
   after it, differs from the vendor factor (rounded to six places). Events at or before a
   collapsed event are out of scope: the ambiguity travels back. #}
select ticker, event_date, factor, factor_chained_check,
       abs(factor - factor_chained_check) as diff
from {{ ref('int_corporate_actions') }}
where kind = 'split'
  and not has_collapsed_at_or_after
  and abs(factor - factor_chained_check) > 1e-6 * greatest(factor, 1.0)

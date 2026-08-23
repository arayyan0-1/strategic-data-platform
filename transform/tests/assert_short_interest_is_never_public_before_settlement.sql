-- A number cannot be public before the date it describes.
-- This is the cheap half of the publication rule. The next test checks the size
-- of the gap. This one checks its direction, which is the error that would make
-- a signal read the future.
select
    settlement_date,
    effective_date,
    ticker
from {{ ref('stg_short_interest') }}
where effective_date is not null
  and effective_date <= settlement_date

-- A short-interest signal on session D uses a settlement whose effective date (the first
-- session on which FINRA had published it) is on or before D. A join on the settlement
-- date would use a number about eight sessions before the market saw it.
select security_key, date, short_as_of
from {{ ref('signals') }}
where short_as_of > date
   or (short_as_of is null
       and (short_ratio is not null or days_to_cover is not null or short_change is not null))

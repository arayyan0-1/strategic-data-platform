-- A line that is not the primary line of its security has no window, so it fails the
-- filter. It never enters a series.
select ticker, date, security_key
from {{ ref('int_universe') }}
where not is_primary_line
  and (in_universe or adv is not null or days_in_window is not null or bars_seen is not null)

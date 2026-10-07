{# A returned row is a non-primary line with a universe flag or a window. #}
select ticker, date, security_key
from {{ ref('int_universe') }}
where not is_primary_line
  and (in_universe or adv is not null or days_in_window is not null or bars_seen is not null)

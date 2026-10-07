{# A returned row is a short-interest signal that uses a settlement published after the session
   (a join on settlement_date would). #}
select security_key, date, short_as_of
from {{ ref('signals') }}
where short_as_of > date
   or (short_as_of is null
       and (short_ratio is not null or days_to_cover is not null or short_change is not null))

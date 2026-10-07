{# A returned row is an event-study row with no dates, more dates than events, or two or more
   dates after the event but no t_post. #}
select event, day, n_events, n_dates, t_post
from {{ ref('event_study') }}
where n_dates < 1
   or n_dates > n_events
   or (day >= 1 and n_dates >= 2 and t_post is null)

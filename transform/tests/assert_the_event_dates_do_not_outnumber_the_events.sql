-- Each row of the event study counts at least one event date, and never more dates than
-- events. A row with two dates or more, after the event, has a t-statistic.
select event, day, n_events, n_dates, t_post
from {{ ref('event_study') }}
where n_dates < 1
   or n_dates > n_events
   or (day >= 1 and n_dates >= 2 and t_post is null)

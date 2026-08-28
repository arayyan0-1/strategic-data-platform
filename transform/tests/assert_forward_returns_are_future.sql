-- No forward return past the last bar of a series. A non-null value here means
-- the panel read a price the vendor has not sent.
with last_session as (
    select security_key, max(date) as d
    from {{ ref('mart_forward_returns') }}
    group by security_key
)
select f.security_key, f.date, f.fwd_ret_1
from {{ ref('mart_forward_returns') }} f
inner join last_session l
    on l.security_key = f.security_key and l.d = f.date
where f.fwd_ret_1 is not null

-- The residual label of one session at D is the residual of the return from D to the
-- next session. Checked on the last 60 sessions.
with days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('mart_signals') }})

), recent as (

    select date from days order by date desc limit 60

)

select f.security_key, f.date, f.fwd_resid_1, r.resid
from {{ ref('mart_forward_residuals') }} f
join days d using (date)
join {{ ref('mart_residuals') }} r
    on r.security_key = f.security_key and r.date = d.next_date
where f.date in (select date from recent)
  and ((f.fwd_resid_1 is null) <> (r.resid is null)
       or abs(f.fwd_resid_1 - r.resid) > 1e-9)

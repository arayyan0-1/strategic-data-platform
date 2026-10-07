{# A returned row is a one-session residual label that differs from style_residuals on the next
   session (last 60 sessions). #}
with days as (

    select date, lead(date) over (order by date) as next_date
    from (select distinct date from {{ ref('signals') }})

), recent as (

    select date from days order by date desc limit 60

)

select f.security_key, f.date, f.fwd_resid_1, r.resid
from {{ ref('forward_residuals') }} f
join days d using (date)
join {{ ref('style_residuals') }} r
    on r.security_key = f.security_key and r.date = d.next_date
where f.date in (select date from recent)
  -- Lets the join read only the recent rows of style_residuals.
  and r.date in (select next_date from days where date in (select date from recent))
  and ((f.fwd_resid_1 is null) <> (r.resid is null)
       or abs(f.fwd_resid_1 - r.resid) > 1e-9)

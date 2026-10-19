{# A returned row is a name and session where resid_reversal_5 or resid_reversal_z_5 differs from
   minus the sum of the residuals dated on that session and the four before, which proves no later
   residual is used (last 60 sessions). #}
with sessions as (

    select date, row_number() over (order by date) as session
    from (select distinct date from {{ ref('signals') }})

), recent as (

    select * from sessions order by date desc limit 60

), resid as (

    select r.security_key, s.session, r.resid, r.resid_z
    from {{ ref('style_residuals') }} r
    join sessions s using (date)
    where s.session >= (select min(session) from recent) - 4

), recomputed as (

    select
        t.security_key, t.date,
        case when count(*) = 5 then -sum(r.resid) end                as resid_reversal_5,
        case when count(r.resid_z) = 5 then -sum(r.resid_z) end      as resid_reversal_z_5
    from {{ ref('residual_signals') }} t
    join recent s using (date)
    left join resid r
        on r.security_key = t.security_key
       and r.session between s.session - 4 and s.session
    group by t.security_key, t.date

)

select c.*, t.resid_reversal_5 as got, t.resid_reversal_z_5 as got_z
from recomputed c
join {{ ref('residual_signals') }} t using (security_key, date)
where (c.resid_reversal_5 is null) <> (t.resid_reversal_5 is null)
   or abs(c.resid_reversal_5 - t.resid_reversal_5) > 1e-9
   or (c.resid_reversal_z_5 is null) <> (t.resid_reversal_z_5 is null)
   or abs(c.resid_reversal_z_5 - t.resid_reversal_z_5) > 1e-9

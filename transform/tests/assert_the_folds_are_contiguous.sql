{# A returned row is a development fold that is not one run of sessions, is out of date order, or
   differs in length by more than one session. #}
with runs as (

    select
        fold,
        min(session) as first_session,
        max(session) as last_session,
        count(*)     as n
    from {{ ref('eval_sessions') }}
    where period = 'development'
    group by fold

)

select *
from runs
where last_session - first_session + 1 <> n
   or first_session <> coalesce(
        (select max(last_session) from runs r where r.fold < runs.fold) + 1, first_session)
   or n - (select min(n) from runs) > 1

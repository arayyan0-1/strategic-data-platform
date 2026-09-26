-- Each development fold is one run of sessions, the folds follow in date order, and
-- their lengths differ by one session at most.
with runs as (

    select
        fold,
        min(session) as first_session,
        max(session) as last_session,
        count(*)     as n
    from {{ ref('mart_eval_sessions') }}
    where period = 'development'
    group by fold

)

select *
from runs
where last_session - first_session + 1 <> n
   or first_session <> coalesce(
        (select max(last_session) from runs r where r.fold < runs.fold) + 1, first_session)
   or n - (select min(n) from runs) > 1

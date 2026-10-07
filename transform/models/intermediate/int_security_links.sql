{# One row per link: a new FIGI on a ticker that continues the old security. basis says whether
   the rule or a reviewed decision made it; root_key is the first key of the chain. Tests are in
   int_key_changes. #}

with recursive links as (

    -- One link for each old key and each new key.
    select
        ticker, link_date, name, cik, old_key, new_key, last_old_bar, first_new_bar, jump,
        vol, case when decision is null then 'rule' else 'reviewed' end as basis
    from {{ ref('int_key_changes') }}
    where linked
    qualify count(*) over (partition by old_key) = 1
        and count(*) over (partition by new_key) = 1

), chains as (

    -- Follow a run of links (A to B to C) back to the first key. Each step goes to an
    -- earlier link, so the recursion stops. A cycle has no first key and drops out.
    select new_key, old_key as root_key, link_date as step_date
    from links
    union all
    select c.new_key, l.old_key, l.link_date
    from chains c
    inner join links l
        on l.new_key    = c.root_key
       and l.link_date  < c.step_date

)

select l.*, c.root_key
from links l
inner join chains c using (new_key)
where c.root_key not in (select new_key from links)

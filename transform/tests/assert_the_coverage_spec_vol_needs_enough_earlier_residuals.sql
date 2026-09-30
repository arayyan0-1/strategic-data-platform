-- A coverage row has an own_vol and a spec_vol only when the name has spec_vol_min_obs
-- residuals before the row, in the universe and in the coverage set together. A forecast from
-- fewer residuals gives false extremes for a new listing.
with history as (

    select security_key, date, false as is_universe
    from {{ ref('coverage_residuals') }}
    where hash(security_key) % 20 = 0

    union all

    select security_key, date, true
    from {{ ref('style_residuals') }}
    where hash(security_key) % 20 = 0
      and security_key in (select security_key from {{ ref('coverage_residuals') }})

), counted as (

    select security_key, date, is_universe,
           row_number() over (partition by security_key order by date) - 1 as n_before
    from history

)

select c.security_key, c.date, h.n_before, c.own_vol, c.spec_vol
from {{ ref('coverage_residuals') }} c
inner join counted h
    on h.security_key = c.security_key and h.date = c.date and not h.is_universe
where (h.n_before >= {{ var('spec_vol_min_obs') }}) <> (c.own_vol is not null)
   or (c.own_vol is null and c.spec_vol is not null)

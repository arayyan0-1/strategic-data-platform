-- A name has an own_vol and a spec_vol only when it has spec_vol_min_obs residuals before
-- the row. A forecast from fewer residuals gives false extremes for a new listing.
with rows as (

    select security_key, date, own_vol, spec_vol,
           row_number() over (partition by security_key order by date) - 1 as n_before
    from {{ ref('style_residuals') }}

)

select security_key, date, n_before, own_vol, spec_vol
from rows
where (n_before >= {{ var('spec_vol_min_obs') }}) <> (own_vol is not null)
   or (own_vol is null and spec_vol is not null)

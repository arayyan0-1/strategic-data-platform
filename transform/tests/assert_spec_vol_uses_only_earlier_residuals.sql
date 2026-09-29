-- Recompute own_vol for a sample of rows from the residuals before the row, with the weight
-- decay^(rows between) written out. The running sums of the model must give the same
-- number. A residual of the row itself, or of a later row, would make them differ.
{% set decay = 0.5 ** (1.0 / var('spec_vol_half_life')) %}
with rows as (

    select security_key, date, resid, own_vol,
           row_number() over (partition by security_key order by date) as k
    from {{ ref('style_residuals') }}

), sample as (

    select security_key, date, k, own_vol
    from rows
    where hash(security_key) % 200 = 0
      and k % 7 = 0
      and k - 1 >= {{ var('spec_vol_min_obs') }}

), expected as (

    select
        s.security_key, s.date, s.own_vol,
        sqrt(sum(power({{ decay }}, s.k - 1 - r.k) * r.resid * r.resid)
             / sum(power({{ decay }}, s.k - 1 - r.k)))              as expected_vol
    from sample s
    join rows r on r.security_key = s.security_key and r.k < s.k
    group by s.security_key, s.date, s.own_vol

)

select *
from expected
where abs(own_vol - expected_vol) > 1e-8 * expected_vol

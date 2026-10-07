{# A returned row is a fund spec_vol that differs from a recomputation over earlier residuals
   (macro_pending rows have no weight), or is set or missing against fund_spec_vol_min_obs. #}
{% set decay = 0.5 ** (1.0 / var('fund_spec_vol_half_life')) %}
with rows as (

    select security_key, date, resid, spec_vol, macro_pending,
           row_number() over (partition by security_key order by date) as k
    from {{ ref('fund_residuals') }}

), sample as (

    select security_key, date, k, spec_vol
    from rows
    where hash(security_key) % 100 = 0 and k % 5 = 0

), expected as (

    select
        s.security_key, s.date, s.spec_vol,
        count(r.k)                                                  as n_before,
        sqrt(sum(power({{ decay }}, s.k - 1 - r.k) * r.resid * r.resid)
             / sum(power({{ decay }}, s.k - 1 - r.k)))              as expected_vol
    from sample s
    left join rows r
        on r.security_key = s.security_key and r.k < s.k and not r.macro_pending
    group by s.security_key, s.date, s.spec_vol

)

select *
from expected
where (n_before >= {{ var('fund_spec_vol_min_obs') }}
       and (spec_vol is null or abs(spec_vol - expected_vol) > 1e-8 * expected_vol))
   or (n_before < {{ var('fund_spec_vol_min_obs') }} and spec_vol is not null)

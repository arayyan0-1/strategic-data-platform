-- Recompute spec_vol for a sample of rows from the residuals before the row, with the
-- weight decay^(rows between) written out. A row with macro_pending has no weight. The
-- running sums of the model must give the same number, and a row with fewer than
-- fund_spec_vol_min_obs earlier weights has none. A residual of the row itself, or of a
-- later row, would make them differ.
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

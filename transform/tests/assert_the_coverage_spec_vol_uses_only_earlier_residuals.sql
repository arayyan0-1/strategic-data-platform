-- Recompute own_vol for a sample of coverage rows from the residuals before the row, with the
-- weight decay^(rows between) written out. The history of a name holds its universe and its
-- coverage residuals together. The running sums of the model must give the same number. A
-- residual of the row itself, or of a later row, would make them differ. spec_vol is own_vol
-- times the regime that style_residuals measured on the universe of the session.
{% set decay = 0.5 ** (1.0 / var('spec_vol_half_life')) %}
with history as (

    select security_key, date, resid
    from {{ ref('coverage_residuals') }}
    where hash(security_key) % 200 = 0

    union all

    select security_key, date, resid
    from {{ ref('style_residuals') }}
    where hash(security_key) % 200 = 0
      and security_key in (select security_key from {{ ref('coverage_residuals') }})

), rows as (

    select *, row_number() over (partition by security_key order by date) as k
    from history

), sample as (

    select r.security_key, r.date, r.k, c.own_vol, c.vol_regime, c.spec_vol
    from rows r
    inner join {{ ref('coverage_residuals') }} c
        on c.security_key = r.security_key and c.date = r.date
    where r.k % 5 = 0
      and r.k - 1 >= {{ var('spec_vol_min_obs') }}

), expected as (

    select
        s.security_key, s.date, s.own_vol, s.vol_regime, s.spec_vol,
        sqrt(sum(power({{ decay }}, s.k - 1 - r.k) * r.resid * r.resid)
             / sum(power({{ decay }}, s.k - 1 - r.k)))              as expected_vol
    from sample s
    inner join rows r on r.security_key = s.security_key and r.k < s.k
    group by s.security_key, s.date, s.own_vol, s.vol_regime, s.spec_vol

), regime as (

    select date, any_value(vol_regime) as vol_regime
    from {{ ref('style_residuals') }}
    group by date

)

select e.*
from expected e
inner join regime g on g.date = e.date
where abs(e.own_vol - e.expected_vol) > 1e-8 * e.expected_vol
   or abs(e.spec_vol - e.expected_vol * g.vol_regime) > 1e-8 * e.spec_vol
   or abs(e.vol_regime - g.vol_regime) > 1e-12

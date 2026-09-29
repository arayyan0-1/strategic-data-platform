-- Recompute vol_regime for a sample of sessions from the sessions before, with the weight
-- decay^(sessions between) written out. z before the regime is resid_z times vol_regime.
-- The first sessions have no z before them, so their regime is 1.
{% set decay = 0.5 ** (1.0 / var('spec_vol_regime_half_life')) %}
with sessions as (

    select date, row_number() over (order by date) as t
    from (select distinct date from {{ ref('style_residuals') }})

), z as (

    select date, avg(power(resid_z * vol_regime, 2)) as mean_z2
    from {{ ref('style_residuals') }}
    where resid_z is not null
    group by date

), regime as (

    select date, any_value(vol_regime) as vol_regime
    from {{ ref('style_residuals') }}
    group by date

), sample as (

    select s.date, s.t, r.vol_regime
    from sessions s
    join regime r on r.date = s.date
    where s.t % 25 = 0

), expected as (

    select
        p.date, p.vol_regime,
        coalesce(sqrt(sum(power({{ decay }}, p.t - 1 - q.t) * z.mean_z2)
                      / nullif(sum(case when z.mean_z2 is not null
                                        then power({{ decay }}, p.t - 1 - q.t) end), 0)),
                 1.0)                                               as expected_regime
    from sample p
    left join sessions q on q.t < p.t
    left join z on z.date = q.date
    group by p.date, p.vol_regime

)

select *
from expected
where abs(vol_regime - expected_regime) > 1e-8 * expected_regime

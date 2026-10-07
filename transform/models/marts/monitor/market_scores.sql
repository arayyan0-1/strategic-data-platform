{# Score of each monitor move, per family, subject, horizon and session: a score of 3 is as rare
   as a normal 3 sigma move. z_raw is trailing (macros/scores.sql); only the map to z uses the
   family's pool. Families: etf, rate, rate_step, relation, specific. #}

{% set step_groups = sql_list('monitor_step_groups') %}
{% set measures = market_rate_measures() %}
{% set names = [] %}
{% for m in measures %}{% do names.append(m[0]) %}{% endfor %}
{% set columns = names | join(', ') %}

with etf as (

    select 'etf' as family, ticker as subject, date,
           ln(adj_close / lag(adj_close) over w) as x
    from {{ ref('market_history') }}
    window w as (partition by ticker order by date)

), etf_moves as (

    -- An ETF under monitor_min_etf_vol (a T-bill fund) has no row: each accrual looks like a
    -- large move.
    select
        family, subject, date, x, 0.0 as min_sd,
        coalesce(count(x) over v = 20
                 and stddev_samp(x) over v * sqrt(252) >= {{ var('monitor_min_etf_vol') }}, false) as elig
    from etf
    window v as (partition by subject order by date rows between 19 preceding and current row)

), defs(measure, grp, unit) as (

    values
    {% for m, label, grp, unit, maturity, stress in measures %}
        ('{{ m }}', '{{ grp }}', '{{ unit }}'){{ "," if not loop.last }}
    {% endfor %}

), level as (

    -- A price moves in proportion to its level, so its changes are log returns.
    -- rate_step holds monitor_step_groups, which move in steps; monitor_tick floors the
    -- volatility of pct measures.
    select d.measure, d.grp, d.unit, l.date,
           case when d.unit in ('fx', 'price') then ln(l.value) else l.value end as x
    from (
        unpivot (select date, {{ columns }} from {{ ref('rates') }})
        on {{ columns }}
        into name measure value value
    ) l
    join defs d using (measure)

), rate_moves as (

    select
        case when grp in ({{ step_groups }}) then 'rate_step' else 'rate' end as family,
        measure as subject, date,
        x - lag(x) over (partition by measure order by date) as x,
        case when unit = 'pct' then {{ var('monitor_tick') }} else 0.0 end as min_sd,
        true as elig
    from level

), relation_moves as (

    select 'relation' as family, relation as subject, date, change_1 as x, 0.0 as min_sd, true as elig
    from {{ ref('market_relation_values') }}

), specific as (

    select 'specific' as family, security_key as subject, 1 as h, date, true as elig,
           resid_z as z_raw
    from {{ ref('style_residuals') }}
    where weight_cap >= {{ var('monitor_big_cap') }} and resid_z is not null

), raw as (

    select family, subject, h, date, elig, z_raw from {{ move_z('etf_moves', [1, 5, 21]) }}
    union all
    select family, subject, h, date, elig, z_raw from {{ move_z('rate_moves', [1]) }}
    union all
    select family, subject, h, date, elig, z_raw from {{ move_z('relation_moves', [21]) }}
    union all
    select family, subject, h, date, elig, z_raw from specific

)

select family, subject, h, date, z_raw, z
from {{ calibrated('raw') }}

{#
  The score of each move of the monitor, one row per family, subject, horizon and
  session, over the whole history. A score is as rare as a move of that many standard
  deviations of a normal distribution: a score of 3 is a move that the family passes on
  about 1 day in 370.

  z_raw is the move over h sessions divided by the volatility known before the last
  session of the move, times the root of h (macros/scores.sql). z is the score: z_raw
  mapped to the normal quantile of the share of the pool of its family that is as large.
  Each z_raw uses trailing data only. The map uses the whole pool, as a table of rarity.

  - etf: the log return of a monitor ETF over 1, 5 and 21 sessions. An ETF whose
    20-session volatility is under monitor_min_etf_vol a year (a T-bill fund) has no row,
    because each accrual looks like a large move.
  - rate and rate_step: the change of a market_rates measure over one session, with the
    volatility at least monitor_tick for a measure in percentage points. rate_step holds
    the groups of the var monitor_step_groups, which move in steps.
  - relation: the change of a market_relation_values relation over 21 sessions.
  - specific: resid_z of the names of a cap of monitor_big_cap or more (style_residuals).
    Its z_raw is the value that the model gives, so the map follows the model.
#}

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

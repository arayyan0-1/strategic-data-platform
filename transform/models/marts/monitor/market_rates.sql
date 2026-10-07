{# One row per FRED measure from core.rates on the last session, values known before it. fresh:
   observed on or after the previous session (chg_1d is null otherwise: a carried value is not a
   move of zero). chg_*: pct points, usd_b billions, fx/price log returns. #}

{% set measures = market_rate_measures() %}

{# The FRED series behind each measure. #}
{% set feeds = {
    'curve_10y_2y': ['DGS10', 'DGS2'],
    'curve_10y_3m': ['DGS10', 'DGS3MO'],
    'curve_30y_5y': ['DGS30', 'DGS5'],
    'fly_2_5_10': ['DGS2', 'DGS5', 'DGS10'],
    'fwd_1y1y': ['DGS2', 'DGS1'],
    'fwd_5y5y': ['DGS10', 'DGS5'],
    'policy_priced_1y': ['DGS1', 'DGS6MO', 'DGS3MO'],
    'bill_effr': ['DGS3MO', 'EFFR'],
    'breakeven_7y': ['DGS7', 'DFII7'],
    'breakeven_20y': ['DGS20', 'DFII20'],
    'breakeven_30y': ['DGS30', 'DFII30'],
    'net_liquidity': ['WALCL', 'WTREGEN', 'RRPONTSYD'],
    'sofr_iorb': ['SOFR', 'IORB'],
    'effr_iorb': ['EFFR', 'IORB'],
    'sofr_tail': ['SOFR99', 'SOFR'],
    'tgcr_rrp': ['TGCRRATE', 'RRPONTSYAWARD'],
    'cp_bill': ['DCPF3M', 'DGS3MO'],
    'vix_term': ['VIXCLS', 'VXVCLS']} %}
{% for id, col in fred_columns().items() %}{% do feeds.update({col: [id]}) %}{% endfor %}

with defs(position, measure, label, grp, unit, maturity, stress) as (

    values
    {% for m, label, grp, unit, maturity, stress in measures %}
        ({{ loop.index }}, '{{ m }}', '{{ label }}', '{{ grp }}', '{{ unit }}',
         {{ maturity if maturity is not none else 'null::integer' }}, '{{ stress }}'){{ "," if not loop.last }}
    {% endfor %}

), long as (

    unpivot (select date, {% for m in measures %}{{ m[0] }}{{ ", " if not loop.last }}{% endfor %}
             from {{ ref('rates') }})
    on {% for m in measures %}{{ m[0] }}{{ ", " if not loop.last }}{% endfor %}
    into name measure value value

), measured as (

    -- A price moves in proportion to its level, so its changes are log returns.
    select l.measure, l.date, l.value,
           case when d.unit in ('fx', 'price') then ln(l.value) else l.value end as x
    from long l
    join defs d using (measure)

), changed as (

    select
        measure, date, value,
        x - lag(x) over w                             as chg_1d,
        x - lag(x, 5) over w                          as chg_1w,
        x - lag(x, 21) over w                         as chg_1m,
        x - lag(x, 252) over w                        as chg_1y,
        lag(value, 21) over w                         as value_1m,
        lag(value, 252) over w                        as value_1y,
        percent_rank() over (partition by measure order by value) as pctile
    from measured
    window w as (partition by measure order by date)

), scaled as (

    select *, row_number() over (partition by measure order by date desc) as back
    from changed

), sessions as (

    select max(date) as last_date,
           max(date) filter (where date < (select max(date) from {{ ref('rates') }})) as prev_date
    from {{ ref('rates') }}

), feeds(measure, series_id) as (

    {% set pairs = [] %}
    {% for m in measures %}{% for id in feeds[m[0]] %}{% do pairs.append((m[0], id)) %}{% endfor %}{% endfor %}
    values
    {% for m, id in pairs %}
        ('{{ m }}', '{{ id }}'){{ "," if not loop.last }}
    {% endfor %}

), observed as (

    -- The last observation of each series that is known before the last session.
    select f.measure, min(o.obs_date) as obs_date
    from feeds f
    left join (
        select series_id, max(date) as obs_date
        from {{ ref('stg_fred__series') }}, sessions
        where value is not null and date < last_date
        group by series_id
    ) o using (series_id)
    group by f.measure

), stamped as (

    select s.*, o.obs_date, coalesce(o.obs_date >= ss.prev_date, false) as fresh
    from scaled s
    join observed o using (measure)
    cross join sessions ss
    where s.back = 1

)

select
    d.position,
    d.measure,
    d.label,
    d.grp,
    d.unit,
    d.maturity,
    d.stress,
    s.date                                                  as last_date,
    s.obs_date,
    s.fresh,
    s.value,
    case when s.fresh then s.chg_1d end                     as chg_1d,
    s.chg_1w,
    s.chg_1m,
    s.chg_1y,
    case when s.fresh then z.z end                          as z_1d,
    s.pctile,
    s.value_1m,
    s.value_1y
from defs d
join stamped s on s.measure = d.measure
left join {{ ref('market_scores') }} z
    on z.family in ('rate', 'rate_step') and z.subject = d.measure and z.h = 1 and z.date = s.date
order by d.position

{#
  Rates, credit, inflation, funding, volatility and financial conditions on the last
  session, one row per measure, from core.rates. Each value is the FRED observation known
  before the session.

  - obs_date is the date of the observation, the oldest of its series for a derived
    measure. FRED can publish a series days late (VIX), and a weekly series (NFCI) has
    one value a week. fresh is true when the observation is from the session before or
    later.
  - chg_1d, chg_1w, chg_1m and chg_1y are changes over 1, 5, 21 and 252 sessions, in
    the unit of the measure (percentage points for the pct unit). chg_1d is null when
    the measure is not fresh, because a carried value is not a move of zero.
  - z_1d is chg_1d over the standard deviation of the daily changes of the 252 sessions
    before, so a move is in units of its own risk.
  - pctile is the share of the history of the lake with a lower or equal value.
  - value_1m and value_1y are the levels 21 and 252 sessions before, for the curve chart.
  - maturity is in months for a point of the nominal or the real Treasury curve.
  - stress says which end is stress, as in market_regime. For a yield it is the high end:
    a higher discount rate.
#}

{% set measures = [
    ('t_1m', 'Treasury 1 month', 'Nominal curve', 'pct', 1, 'high'),
    ('t_3m', 'Treasury 3 months', 'Nominal curve', 'pct', 3, 'high'),
    ('t_6m', 'Treasury 6 months', 'Nominal curve', 'pct', 6, 'high'),
    ('t_1y', 'Treasury 1 year', 'Nominal curve', 'pct', 12, 'high'),
    ('t_2y', 'Treasury 2 years', 'Nominal curve', 'pct', 24, 'high'),
    ('t_3y', 'Treasury 3 years', 'Nominal curve', 'pct', 36, 'high'),
    ('t_5y', 'Treasury 5 years', 'Nominal curve', 'pct', 60, 'high'),
    ('t_7y', 'Treasury 7 years', 'Nominal curve', 'pct', 84, 'high'),
    ('t_10y', 'Treasury 10 years', 'Nominal curve', 'pct', 120, 'high'),
    ('t_20y', 'Treasury 20 years', 'Nominal curve', 'pct', 240, 'high'),
    ('t_30y', 'Treasury 30 years', 'Nominal curve', 'pct', 360, 'high'),
    ('curve_10y_2y', 'Curve 10 years less 2 years', 'Curve', 'pct', none, 'low'),
    ('curve_10y_3m', 'Curve 10 years less 3 months', 'Curve', 'pct', none, 'low'),
    ('real_5y', 'Real yield 5 years (TIPS)', 'Real yields and inflation', 'pct', 60, 'high'),
    ('real_7y', 'Real yield 7 years (TIPS)', 'Real yields and inflation', 'pct', 84, 'high'),
    ('real_10y', 'Real yield 10 years (TIPS)', 'Real yields and inflation', 'pct', 120, 'high'),
    ('real_20y', 'Real yield 20 years (TIPS)', 'Real yields and inflation', 'pct', 240, 'high'),
    ('real_30y', 'Real yield 30 years (TIPS)', 'Real yields and inflation', 'pct', 360, 'high'),
    ('breakeven_5y', 'Breakeven inflation 5 years', 'Real yields and inflation', 'pct', none, 'high'),
    ('breakeven_10y', 'Breakeven inflation 10 years', 'Real yields and inflation', 'pct', none, 'high'),
    ('breakeven_5y5y', 'Inflation 5 years forward, 5 years', 'Real yields and inflation', 'pct', none, 'high'),
    ('fed_funds', 'Effective fed funds', 'Funding', 'pct', none, 'high'),
    ('sofr', 'SOFR', 'Funding', 'pct', none, 'high'),
    ('bill_4w', 'T-bill 4 weeks (discount)', 'Funding', 'pct', none, 'high'),
    ('hy_oas', 'High-yield spread (ICE BofA OAS)', 'Credit', 'pct', none, 'high'),
    ('ig_oas', 'Investment-grade spread (ICE BofA OAS)', 'Credit', 'pct', none, 'high'),
    ('vix', 'VIX', 'Volatility', 'index', none, 'high'),
    ('vix_3m', 'VIX3M', 'Volatility', 'index', none, 'high'),
    ('vix_term', 'VIX over VIX3M', 'Volatility', 'ratio', none, 'high'),
    ('dollar', 'Broad dollar index', 'Conditions', 'index', none, 'high'),
    ('nfci', 'Chicago Fed financial conditions', 'Conditions', 'index', none, 'high'),
    ('stlfsi', 'St. Louis Fed financial stress', 'Conditions', 'index', none, 'high'),
] %}

{# The FRED series behind each measure. #}
{% set feeds = {
    'curve_10y_2y': ['DGS10', 'DGS2'],
    'curve_10y_3m': ['DGS10', 'DGS3MO'],
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

), changed as (

    select
        measure, date, value,
        value - lag(value) over w                     as chg_1d,
        value - lag(value, 5) over w                  as chg_1w,
        value - lag(value, 21) over w                 as chg_1m,
        value - lag(value, 252) over w                as chg_1y,
        lag(value, 21) over w                         as value_1m,
        lag(value, 252) over w                        as value_1y,
        percent_rank() over (partition by measure order by value) as pctile
    from long
    window w as (partition by measure order by date)

), scaled as (

    select
        *,
        stddev_samp(chg_1d) over (partition by measure order by date
            rows between 252 preceding and 1 preceding) as sd_1d,
        row_number() over (partition by measure order by date desc) as back
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
    case when s.fresh then s.chg_1d / nullif(s.sd_1d, 0) end as z_1d,
    s.pctile,
    s.value_1m,
    s.value_1y
from defs d
join stamped s on s.measure = d.measure
order by d.position

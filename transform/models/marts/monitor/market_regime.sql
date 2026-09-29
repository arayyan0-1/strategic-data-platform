{#
  The regime of the stock market on the last session, one row per measure, against its
  own history: pctile is the share of the sessions with a lower or equal value, z is
  the distance from the mean in standard deviations, and change_21 is the change over
  21 sessions. stress says which end of the range is the stress end, so a reader and
  market_exceptions can tell good from bad. The rate, spread and volatility measures come
  from core.rates, so each is the FRED value known before the session. The history of
  each measure is the history of the lake, and it is shorter for the credit spreads,
  which FRED serves from 2023 only.
#}

with series as (

    unpivot (
        select date, pct_above_ma50, pct_above_ma200, net_highs, dispersion_21,
               resid_dispersion_21, avg_corr_21, ew_vol_21, absorption, style_r2_21,
               median_spread, up_volume_share_21
        from {{ ref('market_breadth') }}
    )
    on pct_above_ma50, pct_above_ma200, net_highs, dispersion_21, resid_dispersion_21,
       avg_corr_21, ew_vol_21, absorption, style_r2_21, median_spread, up_volume_share_21
    into name metric value value

    union all

    unpivot (
        select date, curve_10y_3m, real_10y, hy_oas, ig_oas, vix, vix_term, nfci
        from {{ ref('rates') }}
    )
    on curve_10y_3m, real_10y, hy_oas, ig_oas, vix, vix_term, nfci
    into name metric value value

), defs(metric, label, stress) as (

    values
        ('pct_above_ma50', 'Share above the 50-day average', 'low'),
        ('pct_above_ma200', 'Share above the 200-day average', 'low'),
        ('net_highs', '52-week highs less lows', 'low'),
        ('dispersion_21', 'Dispersion of returns (21d)', 'high'),
        ('resid_dispersion_21', 'Stock-specific dispersion (21d)', 'high'),
        ('avg_corr_21', 'Average correlation (21d)', 'high'),
        ('ew_vol_21', 'Index volatility (21d, equal weight)', 'high'),
        ('absorption', 'Absorption ratio (first principal component)', 'high'),
        ('style_r2_21', 'Share explained by the factor model (21d)', 'high'),
        ('median_spread', 'Median bid-ask spread', 'high'),
        ('up_volume_share_21', 'Up-volume share (21d)', 'low'),
        ('curve_10y_3m', 'Treasury curve, 10 years less 3 months (%)', 'low'),
        ('real_10y', '10-year real yield (%)', 'high'),
        ('hy_oas', 'High-yield credit spread (%)', 'high'),
        ('ig_oas', 'Investment-grade credit spread (%)', 'high'),
        ('vix', 'VIX', 'high'),
        ('vix_term', 'VIX over VIX3M (above 1 is inverted)', 'high'),
        ('nfci', 'Chicago Fed financial conditions (above 0 is tight)', 'high')

), stats as (

    select
        metric,
        arg_max(value, date)                                   as value,
        max(date)                                              as last_date,
        avg(value)                                             as mean,
        stddev_samp(value)                                     as sd
    from series
    where value is not null
    group by metric

), ranked as (

    select s.metric,
           avg((x.value <= s.value)::int)                        as pctile,
           arg_max(x.value, x.date) filter (where x.date <= (
               select max(date) from series) - interval 30 day)  as value_before
    from stats s
    join series x using (metric)
    where x.value is not null
    group by s.metric

)

select
    d.metric,
    d.label,
    d.stress,
    s.last_date,
    s.value,
    r.pctile,
    (s.value - s.mean) / nullif(s.sd, 0)                       as z,
    s.value - r.value_before                                   as change_21
from defs d
join stats s using (metric)
join ranked r using (metric)

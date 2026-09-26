{#
  What is unusual on the last session, one row per item, most unusual first. score is
  the size of the surprise in standard deviations. A percentile maps to a score of 2 at
  the limit and 3 at the extreme. The limits are the vars monitor_z, monitor_pct and
  monitor_big_cap. direction is 1 for a move up or a calm reading, and -1 for a move down
  or a warning (a crowded factor, a high-volatility regime, the stress end of a
  measure). An ETF with less than 2% volatility a year (T-bills) makes each
  accrual look like a large move in sigma, so it is left out. An industry return is
  relative to the market and can trend for years, so its drawdown is not an exception.
#}
{% set z = var('monitor_z') %}
{% set pct = var('monitor_pct') %}

with names as (

    -- The name of each industry factor. Other has no SIC range in the seed.
    select distinct lower(industry) as factor, industry_name as name
    from {{ ref('ff12_industries') }}
    union all
    select 'other', 'Other'

), risk as (

    select r.*,
           case when r.family = 'industry' then coalesce(n.name, r.factor) || ' (industry)'
                else r.factor end as label
    from {{ ref('mart_factor_risk') }} r
    left join names n on r.family = 'industry' and n.factor = r.factor

), items as (

    select 'cross-asset' as area, ticker as subject,
           printf('%s %+.1f%% today, %+.1f sigma', label, 100 * ret_1d, z_1d) as message,
           abs(z_1d) as score, sign(z_1d) as direction
    from {{ ref('mart_market_board') }}
    where abs(z_1d) >= {{ z }} and vol_20d >= 0.02

    union all
    select 'cross-asset', ticker,
           printf('%s %+.1f%% over a month, %+.1f sigma', label, 100 * ret_1m, z_1m),
           abs(z_1m), sign(z_1m)
    from {{ ref('mart_market_board') }}
    where abs(z_1m) >= {{ z }} + 0.5 and vol_20d >= 0.02

    union all
    select 'factor', family || ': ' || factor,
           printf('%s factor %+.2f%% today, %+.1f sigma', label, 100 * ret_1d, z_1d),
           abs(z_1d), sign(z_1d)
    from risk
    where family in ('style', 'industry') and abs(z_1d) >= {{ z }}

    union all
    select 'factor', family || ': ' || factor,
           printf('%s factor %+.1f%% over a month, %+.1f sigma', label, 100 * ret_1m, z_1m),
           abs(z_1m), sign(z_1m)
    from risk
    where family in ('style', 'industry') and abs(z_1m) >= {{ z }}

    union all
    select 'factor risk', family || ': ' || factor,
           printf('%s volatility %.1f%% a year, above %.0f%% of its history',
                  label, 100 * vol_63, 100 * vol_pctile),
           2 + (vol_pctile - (1 - {{ pct }})) / {{ pct }}, -1
    from risk
    where family in ('style', 'industry') and vol_pctile >= 1 - {{ pct }}

    union all
    select 'factor risk', family || ': ' || factor,
           printf('%s drawdown %.1f%%, deeper than %.0f%% of its history',
                  factor, 100 * drawdown, 100 * dd_pctile),
           2 + (dd_pctile - (1 - {{ pct }})) / {{ pct }}, -1
    from {{ ref('mart_factor_risk') }}
    where family = 'style' and factor <> 'market'
      and dd_pctile >= 1 - {{ pct }} and drawdown < -0.05

    union all
    select 'crowding', factor,
           printf('%s crowded: short interest leans on its short leg, %+.1f sigma',
                  factor, crowding_z),
           crowding_z, -1
    from {{ ref('mart_factor_risk') }}
    where crowding_z >= {{ z }}

    union all
    select 'factor risk', family || ': ' || factor,
           printf('%s correlation with the market %.2f over 63 sessions against %.2f over 252',
                  factor, corr_63, corr_252),
           5 * abs(corr_63 - corr_252), sign(corr_63 - corr_252)
    from {{ ref('mart_factor_risk') }}
    where family = 'style' and factor <> 'market' and abs(corr_63 - corr_252) >= 0.4

    union all
    select 'regime', metric,
           printf('%s at percentile %.0f of its history (%s is stress)',
                  label, 100 * pctile, stress),
           2 + (greatest(pctile, 1 - pctile) - (1 - {{ pct }})) / {{ pct }},
           case when (stress = 'high') = (pctile > 0.5) then -1 else 1 end
    from {{ ref('mart_market_regime') }}
    where pctile >= 1 - {{ pct }} or pctile <= {{ pct }}

    union all
    select 'relation', relation,
           printf('%s %+.1f%% over a month, %+.1f sigma', label,
                  100 * change_21, z_21),
           abs(z_21), sign(z_21)
    from {{ ref('mart_market_relations') }}
    where date = (select max(date) from {{ ref('mart_market_relations') }})
      and kind <> 'corr' and abs(z_21) >= {{ z }}

    union all
    select 'relation', relation,
           printf('%s at %.2f, %+.2f over a month', label, value, change_21),
           abs(z_21), sign(z_21)
    from {{ ref('mart_market_relations') }}
    where date = (select max(date) from {{ ref('mart_market_relations') }})
      and kind = 'corr' and abs(z_21) >= {{ z }}

    union all
    select 'stock-specific', ticker,
           printf('%s %+.1f%% stock-specific (%+.1f sigma), %+.1f%% in all', coalesce(name, ticker),
                  100 * resid, resid_z, 100 * ret),
           abs(resid_z) / 2, sign(resid_z)
    from {{ ref('mart_market_movers') }}
    where cap >= {{ var('monitor_big_cap') }} and abs(resid_z) >= 2 * {{ z }}

)

select
    (select max(date) from {{ ref('mart_market_breadth') }}) as date,
    *
from items
where score is not null

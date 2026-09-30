{#
  What is unusual on the last session, one row per item, most unusual first. score is
  the size of the surprise in sigma. For a move it is a score (market_scores): a score of
  3 is as rare as a 3 sigma move of a normal distribution on the history of its family,
  about 1 day in 370. A percentile maps to a score of 2 at the limit and 3 at the
  extreme. The limits are the vars monitor_z, monitor_pct and monitor_big_cap.
  direction is 1 for a move up or a calm reading, and -1 for a move down or a warning (a
  crowded factor, a high-volatility regime, the stress end of a measure, a rise in a
  yield, a spread or volatility). An ETF with a volatility under the var
  monitor_min_etf_vol a year (T-bills) has no score, because each accrual looks like a
  large move. An industry return is relative to the market and can trend for years, so
  its drawdown is not an exception. A rates group in the var monitor_step_groups moves
  in steps, so it is not an exception.
#}
{% set z = var('monitor_z') %}
{% set pct = var('monitor_pct') %}

with risk as (

    select * replace (case when family = 'industry' then label || ' (industry)'
                           else label end as label)
    from {{ ref('factor_risk') }}

), items as (

    select 'cross-asset' as area, ticker as subject,
           printf('%s %+.1f%% today, %+.1f sigma', label, 100 * ret_1d, z_1d) as message,
           abs(z_1d) as score, sign(z_1d) as direction
    from {{ ref('market_board') }}
    where abs(z_1d) >= {{ z }}

    union all
    select 'cross-asset', ticker,
           printf('%s %+.1f%% over a month, %+.1f sigma', label, 100 * ret_1m, z_1m),
           abs(z_1m), sign(z_1m)
    from {{ ref('market_board') }}
    where abs(z_1m) >= {{ z }}

    union all
    select 'factor', family || ': ' || factor,
           printf('%s %+.2f%% today, %+.1f sigma', label, 100 * ret_1d, z_1d),
           abs(z_1d), sign(z_1d)
    from risk
    where family in ('style', 'industry') and abs(z_1d) >= {{ z }}

    union all
    select 'factor', family || ': ' || factor,
           printf('%s %+.1f%% over a month, %+.1f sigma', label, 100 * ret_1m, z_1m),
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
                  label, 100 * drawdown, 100 * dd_pctile),
           2 + (dd_pctile - (1 - {{ pct }})) / {{ pct }}, -1
    from risk
    where family = 'style' and factor <> 'market'
      and dd_pctile >= 1 - {{ pct }} and drawdown < -0.05

    union all
    select 'crowding', factor,
           printf('%s crowded: short interest leans on its short leg, %+.1f sigma',
                  label, crowding_z),
           crowding_z, -1
    from risk
    where crowding_z >= {{ z }}

    union all
    select 'factor risk', family || ': ' || factor,
           printf('%s correlation with the market %.2f over 63 sessions against %.2f over 252',
                  label, corr_63, corr_252),
           5 * abs(corr_63 - corr_252), sign(corr_63 - corr_252)
    from risk
    where family = 'style' and factor <> 'market' and abs(corr_63 - corr_252) >= 0.4

    union all
    select 'regime', metric,
           printf('%s at percentile %.0f of its history (%s is stress)',
                  label, 100 * pctile, stress),
           2 + (greatest(pctile, 1 - pctile) - (1 - {{ pct }})) / {{ pct }},
           case when (stress = 'high') = (pctile > 0.5) then -1 else 1 end
    from {{ ref('market_regime') }}
    where pctile >= 1 - {{ pct }} or pctile <= {{ pct }}

    union all
    -- One item per group, the largest move, because a curve moves as one.
    select 'rates', measure,
           printf('%s %s, %+.1f sigma%s', label,
                  case unit
                      when 'pct' then printf('%+.0f bp to %.2f%%', 100 * chg_1d, value)
                      when 'usd_b' then printf('%+.0f bn to %.0f bn', chg_1d, value)
                      when 'fx' then printf('%+.2f%% to %.4f', 100 * chg_1d, value)
                      when 'price' then printf('%+.1f%% to %.2f', 100 * chg_1d, value)
                      else printf('%+.2f to %.2f', chg_1d, value) end,
                  z_1d,
                  case when n_group > 1 then printf(' (and %d more in %s)', n_group - 1, lower(grp))
                       else '' end),
           abs(z_1d),
           case when stress = 'high' then -sign(z_1d) else sign(z_1d) end
    from (
        select *,
               count(*) over (partition by grp) as n_group,
               row_number() over (partition by grp order by abs(z_1d) desc) as rank
        from {{ ref('market_rates') }}
        where grp not in ({{ sql_list('monitor_step_groups') }})
          and abs(z_1d) >= {{ z }}
    )
    where rank = 1

    union all
    select 'relation', relation,
           printf('%s %+.1f%% over a month, %+.1f sigma', label,
                  100 * change_21, z_21),
           abs(z_21), sign(z_21)
    from {{ ref('market_relations') }}
    where date = (select max(date) from {{ ref('market_relations') }})
      and kind <> 'corr' and abs(z_21) >= {{ z }}

    union all
    select 'relation', relation,
           printf('%s at %.2f, %+.2f over a month', label, value, change_21),
           abs(z_21), sign(z_21)
    from {{ ref('market_relations') }}
    where date = (select max(date) from {{ ref('market_relations') }})
      and kind = 'corr' and abs(z_21) >= {{ z }}

    union all
    select 'stock-specific', r.ticker,
           printf('%s %+.1f%% stock-specific (%+.1f sigma), %+.1f%% in all', coalesce(u.name, r.ticker),
                  100 * r.resid, s.z, 100 * r.ret),
           abs(s.z), sign(s.z)
    from {{ ref('market_scores') }} s
    join {{ ref('style_residuals') }} r on r.security_key = s.subject and r.date = s.date
    left join {{ ref('int_universe') }} u on u.ticker = r.ticker and u.date = r.date
    where s.family = 'specific' and s.h = 1
      and s.date = (select max(date) from {{ ref('style_residuals') }})
      and abs(s.z) >= {{ z }}

)

select
    (select max(date) from {{ ref('market_breadth') }}) as date,
    *
from items
where score is not null

{#
  The residual labels: the forward return of each in-universe name less the part that
  the style and industry model explains, over 1, 5 and 21 sessions. The exposures are
  those of D and the factor returns are those of the window after D. The factor part is
  the sum of the daily factor returns, so the label for one session equals the
  residual in mart_residuals, and the labels for 5 and 21 sessions leave out the
  compounding of the factor returns.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}
{% set horizons = [1, 5, 21] %}

with sessions as (

    select date, row_number() over (order by date) as session
    from (select distinct date from {{ ref('mart_signals') }})

), factors as (

    -- A factor return is dated on the session that earns it.
    select s.session, s.date, f.* exclude (date)
    from sessions s
    left join {{ ref('mart_factor_style') }} f using (date)

), windows as (

    -- The factor returns of the h sessions after each session, summed. A window with a
    -- missing session has no value.
    select
        date,
        {%- for h in horizons %}
        count(market) over w{{ h }} = {{ h }}                  as full_{{ h }},
        sum(market) over w{{ h }}                              as market_{{ h }},
        {%- for s in styles %}
        sum({{ s }}) over w{{ h }}                             as {{ s }}_{{ h }},
        {%- endfor %}
        {%- for c in industries %}
        sum(ind_{{ c | lower }}) over w{{ h }}                 as ind_{{ c | lower }}_{{ h }}{{ "," if not (loop.last and h == horizons[-1]) }}
        {%- endfor %}
        {%- endfor %}
    from factors
    window
        {%- for h in horizons %}
        w{{ h }} as (order by session rows between 1 following and {{ h }} following){{ "," if not loop.last }}
        {%- endfor %}

)

select
    e.security_key,
    e.date,
    {%- for h in horizons %}
    case when w.full_{{ h }} then
        f.fwd_ret_{{ h }}
        - w.market_{{ h }}
        - coalesce(case e.industry
            {%- for c in industries %}
            when '{{ c }}' then w.ind_{{ c | lower }}_{{ h }}
            {%- endfor %}
          end, 0)
        - ({% for s in styles %}e.z_{{ s }} * w.{{ s }}_{{ h }}{{ ' + ' if not loop.last }}{% endfor %})
    end as fwd_resid_{{ h }}{{ "," if not loop.last }}
    {%- endfor %}
from {{ ref('mart_style_exposures') }} e
inner join windows w using (date)
left join {{ ref('mart_forward_returns') }} f using (security_key, date)

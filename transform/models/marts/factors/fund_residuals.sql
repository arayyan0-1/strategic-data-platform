{#
  The return of each fund split into the parts of its exposures (fund_exposures) and a
  residual. One row for each fund and session that a fit covers. A part is the loading of
  the fit of fit_date times the factor return of the session. The parts are market, industry
  (industry_part), the eight styles (style_part) and the macro factors of fund_macro
  (macro_part). alpha is the intercept of the fit, the daily constant of the fund (the carry
  of a bill fund). It is not a factor part. So the return is the four parts plus alpha plus
  resid.

  macro_pending is true when a macro factor of fund_macro has no observation yet on the
  session (macro_factor_returns). The missing change adds 0 to macro_part. So the residual
  of that row still holds the move of that factor, and it is not a move that is specific to
  the fund. The row has no weight in spec_vol of any later row.

  spec_vol is the forecast of the volatility of the residual, and resid_z is the residual in
  units of it. spec_vol is the root mean square of the residuals of the fund before the row.
  The weight of a residual halves every fund_spec_vol_half_life rows. spec_vol is null with
  fewer than fund_spec_vol_min_obs earlier residuals. A row with macro_pending does not
  count as a residual.
#}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}
{% set macro = var('fund_macro') %}
{# The running sums below hold the weight decay^(-k), which grows fast with k. A half-life
   below 2 leaves too little room before it overflows, about 2,000 rows. #}
{% if var('fund_spec_vol_half_life') < 2 %}
    {{ exceptions.raise_compiler_error('fund_spec_vol_half_life must be 2 or more.') }}
{% endif %}
{% set decay = 0.5 ** (1.0 / var('fund_spec_vol_half_life')) %}

with parts as (

    select
        r.security_key,
        r.ticker,
        r.date,
        e.fit_date,
        r.ret,
        e.intercept                                                 as alpha,
        e.market * f.market                                         as market_part,
        {% for c in industries %}e.ind_{{ c | lower }} * f.ind_{{ c | lower }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                    as industry_part,
        {% for s in styles %}e.{{ s }} * f.{{ s }}{{ ' + ' if not loop.last }}{% endfor %}
                                                                    as style_part,
        {% for m in macro %}coalesce(e.{{ m }} * x.{{ m }}, 0){{ ' + ' if not loop.last }}{% else %}0.0{% endfor %}
                                                                    as macro_part,
        {% if macro %}list_has_any(x.pending, [{% for m in macro %}'{{ m }}'{{ ', ' if not loop.last }}{% endfor %}]){% else %}false{% endif %}
                                                                    as macro_pending
    from {{ ref('int_fund_returns') }} r
    inner join {{ ref('fund_exposures') }} e
        on e.security_key = r.security_key
       and r.date between e.fit_date and e.last_date
    inner join {{ ref('style_factor_returns') }} f on f.date = r.date
    inner join {{ ref('macro_factor_returns') }} x on x.date = r.date
    where r.ret is not null and f.market is not null

), resid as (

    select
        *,
        ret - alpha - market_part - industry_part - style_part - macro_part as resid,
        row_number() over (partition by security_key order by date) as k
    from parts

), own as (

    -- One running sum holds each residual with the weight decay^(-k), where k is the
    -- position of the residual in the history of the fund. The same sum over the weights
    -- alone divides it, so the scale decay^(-k) cancels. A row with macro_pending has no
    -- weight.
    select
        *,
        case when count(*) filter (where not macro_pending) over prev
                  >= {{ var('fund_spec_vol_min_obs') }}
             then sqrt(sum(case when not macro_pending
                                then resid * resid * power({{ decay }}, -k) end) over prev
                       / sum(case when not macro_pending
                                  then power({{ decay }}, -k) end) over prev)
        end                                                         as spec_vol
    from resid
    window prev as (partition by security_key order by date
                    rows between unbounded preceding and 1 preceding)

)

select
    security_key,
    ticker,
    date,
    fit_date,
    ret,
    alpha,
    market_part,
    industry_part,
    style_part,
    macro_part,
    resid,
    spec_vol,
    resid / nullif(spec_vol, 0)                                     as resid_z,
    macro_pending
from own

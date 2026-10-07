{# Return of each fund split into factor parts (fit loadings times the session's factor returns),
   alpha (the fit intercept) and a residual. macro_pending: a macro factor is unobserved, so the
   residual holds its move and the row has no weight in later spec_vol. #}
{% set styles = ['size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                 'dividend_yield', 'high_52w'] %}
{% set industries = ['NoDur', 'Durbl', 'Manuf', 'Enrgy', 'Chems', 'BusEq', 'Telcm',
                     'Utils', 'Shops', 'Hlth', 'Money', 'Other', 'Unknown'] %}
{% set macro = var('fund_macro') %}
{# The running sums hold the weight decay^(-k), which overflows after enough rows; a half-life
   below 2 overflows too soon. #}
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

    -- One running sum holds each residual with weight decay^(-k), k its position in the fund's
    -- history; the same sum over the weights alone divides it, so the scale cancels. A
    -- macro_pending row has no weight.
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

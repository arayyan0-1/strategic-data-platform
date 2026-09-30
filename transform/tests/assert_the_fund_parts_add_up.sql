-- Recompute the factor part of a sample of fund rows with the loadings and the factor
-- returns in long form, and compare it with the four parts. A factor that the parts
-- leave out or count twice would make them differ. A macro factor with no change adds 0.
-- The return must equal the parts plus alpha plus the residual.
{% set model = ['market', 'size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                'dividend_yield', 'high_52w', 'ind_nodur', 'ind_durbl', 'ind_manuf', 'ind_enrgy',
                'ind_chems', 'ind_buseq', 'ind_telcm', 'ind_utils', 'ind_shops', 'ind_hlth',
                'ind_money', 'ind_other', 'ind_unknown'] %}
{% set names = model + var('fund_macro') %}
{% set ret_cols = [] %}
{% for c in model %}{% do ret_cols.append('f.' ~ c) %}{% endfor %}
{% for c in var('fund_macro') %}{% do ret_cols.append('m.' ~ c) %}{% endfor %}
with loadings as (

    unpivot (
        select security_key, fit_date, {{ names | join(', ') }}
        from {{ ref('fund_exposures') }}
        where hash(security_key) % 25 = 0
    )
    on {{ names | join(', ') }}
    into name factor value loading

), returns as (

    unpivot (
        select f.date, {{ ret_cols | join(', ') }}
        from {{ ref('style_factor_returns') }} f
        inner join {{ ref('macro_factor_returns') }} m using (date)
    )
    on {{ names | join(', ') }}
    into name factor value ret

), expected as (

    select r.security_key, r.date, r.ret, r.alpha, r.resid,
           r.market_part + r.industry_part + r.style_part + r.macro_part   as parts,
           sum(l.loading * coalesce(x.ret, 0))                             as expected_parts
    from {{ ref('fund_residuals') }} r
    inner join loadings l on l.security_key = r.security_key and l.fit_date = r.fit_date
    left join returns x on x.date = r.date and x.factor = l.factor
    where hash(r.security_key) % 25 = 0
    group by r.security_key, r.date, r.ret, r.alpha, r.resid, r.market_part, r.industry_part,
             r.style_part, r.macro_part

)

select *
from expected
where abs(parts - expected_parts) > 1e-9
   or abs(ret - alpha - expected_parts - resid) > 1e-9

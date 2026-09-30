-- The loadings that a fund earns on a session come from a fit that ends before it.
--  - Each fit ends on the session before its first session, and it reads at most fund_window
--    rows.
--  - Every residual row lies between the first and the last session of its fit.
--  - A ridge fit with a free intercept has a mean return equal to the intercept plus the
--    loadings times the mean factor returns, over the rows that it reads. The test
--    recomputes both sides from the returns and the factor returns of the fund_window
--    sessions that end on window_end, for a sample of funds. A fit that read another row
--    would break the equality.
{% set macro = var('fund_macro') %}
{% set model = ['market', 'size', 'liquidity', 'beta', 'momentum', 'reversal', 'volatility',
                'dividend_yield', 'high_52w', 'ind_nodur', 'ind_durbl', 'ind_manuf', 'ind_enrgy',
                'ind_chems', 'ind_buseq', 'ind_telcm', 'ind_utils', 'ind_shops', 'ind_hlth',
                'ind_money', 'ind_other', 'ind_unknown'] %}
{% set names = model + macro %}
{% set cols = [] %}
{% for c in model %}{% do cols.append('f.' ~ c) %}{% endfor %}
{% for c in macro %}{% do cols.append('m.' ~ c) %}{% endfor %}
with sessions as (

    select date, row_number() over (order by date) as t, lag(date) over (order by date) as prev_date
    from (select distinct date from {{ ref('style_factor_returns') }})

), complete as (

    select s.t, f.date, {{ names | join(', ') }}
    from (
        select f.date, {{ cols | join(', ') }}
        from {{ ref('style_factor_returns') }} f
        inner join {{ ref('macro_factor_returns') }} m using (date)
    ) f
    inner join sessions s on s.date = f.date
    where true
    {% for c in names %}
      and f.{{ c }} is not null
    {% endfor %}

), window_stats as (

    select
        e.security_key, e.fit_date,
        count(r.ret)                                                as expected_n,
        avg(r.ret)                                                  as mean_ret,
        {% for c in names %}avg(c.{{ c }}) as a_{{ c }}{{ ',' if not loop.last }}
        {% endfor %}
    from {{ ref('fund_exposures') }} e
    inner join sessions w on w.date = e.window_end
    inner join complete c on c.t between w.t - {{ var('fund_window') }} + 1 and w.t
    inner join {{ ref('int_fund_returns') }} r
        on r.security_key = e.security_key and r.date = c.date and r.ret is not null
    where hash(e.security_key::varchar) % 20 = 0
    group by e.security_key, e.fit_date

)

select e.security_key, e.fit_date, e.n_obs, s.expected_n, 'the fit does not read its window' as problem
from {{ ref('fund_exposures') }} e
inner join window_stats s on s.security_key = e.security_key and s.fit_date = e.fit_date
where e.n_obs <> s.expected_n
   or abs(s.mean_ret - (e.intercept
          {% for c in names %} + e.{{ c }} * s.a_{{ c }}{% endfor %})) > 1e-9

union all

select e.security_key, e.fit_date, e.n_obs, null, 'the fit does not end the session before it starts'
from {{ ref('fund_exposures') }} e
inner join sessions s on s.date = e.fit_date
where e.window_end <> s.prev_date or e.n_obs > {{ var('fund_window') }}

union all

select r.security_key, r.fit_date, null, null, 'a row lies outside its fit'
from {{ ref('fund_residuals') }} r
inner join {{ ref('fund_exposures') }} e
    on e.security_key = r.security_key and e.fit_date = r.fit_date
where r.date < e.fit_date or r.date > e.last_date or r.date <= e.window_end

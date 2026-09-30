-- A macro factor is pending on a session exactly when its series has no observation on or
-- after the session, and a fund row has macro_pending exactly when a factor of fund_macro
-- is pending. The last observation of each series is read from the series itself.
{% set defs = macro_factors() %}
with ends as (

    select series_id, max(date) as last_date
    from {{ ref('stg_fred__series') }}
    where value is not null
    group by series_id

), expected as (

    {% for name, d in defs.items() %}
    select '{{ name }}' as factor, m.date, list_contains(m.pending, '{{ name }}') as actual,
           m.date > e.last_date as expected
    from {{ ref('macro_factor_returns') }} m
    cross join ends e
    where e.series_id = '{{ d[0] }}'
    union all
    {% endfor %}
    select 'd_curve_10y_2y', m.date, list_contains(m.pending, 'd_curve_10y_2y'),
           m.date > least(a.last_date, b.last_date)
    from {{ ref('macro_factor_returns') }} m
    cross join (select last_date from ends where series_id = 'DGS10') a
    cross join (select last_date from ends where series_id = 'DGS2') b

), fund_rows as (

    select 'macro_pending' as factor, r.date, r.macro_pending as actual,
           r.date > least(
               {% for m in var('fund_macro') %}(select last_date from ends where series_id = '{{ defs[m][0] }}'){{ ', ' if not loop.last }}{% endfor %}
           ) as expected
    from {{ ref('fund_residuals') }} r

)

select * from expected where actual <> expected
union all
select * from fund_rows where actual <> expected

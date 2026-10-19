{# Signals on the stock-specific return of the style model, one row per security and session
   with a residual. Separate from signals because the style model reads signals. A signal over k
   sessions needs a residual on each of the last k sessions. #}
{%- set lookbacks = [1, 5, 21] %}

with sessions as (

    select date, row_number() over (order by date) as session
    from (select distinct date from {{ ref('signals') }})

), resid as (

    select r.security_key, r.date, s.session, r.resid, r.resid_z
    from {{ ref('style_residuals') }} r
    join sessions s using (date)

)

select
    security_key,
    date,
    {%- for k in lookbacks %}
    case when count(*) over w{{ k }} = {{ k }}
              and session - min(session) over w{{ k }} = {{ k - 1 }}
         then -sum(resid) over w{{ k }} end                         as resid_reversal_{{ k }},
    {%- endfor %}
    {%- for k in lookbacks %}
    case when count(resid_z) over w{{ k }} = {{ k }}
              and session - min(session) over w{{ k }} = {{ k - 1 }}
         then -sum(resid_z) over w{{ k }} end                       as resid_reversal_z_{{ k }}{{ "," if not loop.last }}
    {%- endfor %}
from resid
window
    {%- for k in lookbacks %}
    w{{ k }} as (partition by security_key order by date
                 rows between {{ k - 1 }} preceding and current row){{ "," if not loop.last }}
    {%- endfor %}

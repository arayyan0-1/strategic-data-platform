-- Recompute each macro change for the sessions where the series has an observation dated
-- on the session and on the session before: the later value less the earlier one, or the
-- log of their ratio. The last session counts, so the change of a session does not wait
-- for a later row.
{% set defs = macro_factors() %}
with sessions as (

    select date, lag(date) over (order by date) as prev_date
    from (select distinct date from {{ ref('int_universe') }})

), observed as (

    select series_id, date, value
    from {{ ref('stg_fred__series') }}
    where value is not null

), expected as (

    {% for name, d in defs.items() %}
    select
        '{{ name }}' as factor, s.date, m.{{ name }} as actual,
        {% if d[1] == 'log' %}
        case when o.value > 0 and p.value > 0 then ln(o.value / p.value) end
        {% else %}
        o.value - p.value
        {% endif %}                                                 as expected
    from sessions s
    inner join {{ ref('macro_factor_returns') }} m on m.date = s.date
    inner join observed o on o.series_id = '{{ d[0] }}' and o.date = s.date
    inner join observed p on p.series_id = '{{ d[0] }}' and p.date = s.prev_date
    {{ 'union all' if not loop.last }}
    {% endfor %}

)

select *
from expected
where actual is null or expected is null or abs(actual - expected) > 1e-9

{# Daily change of each macro series (percentage points for yields and spreads, log points
   otherwise), for the fund regressions. After a series ends the change is pending: null, not 0,
   and listed in pending. #}

{% set defs = macro_factors() %}
{% set ids = defs.values() | map('first') | list %}

with sessions as (

    select date
    from (select distinct date from {{ ref('int_universe') }})

), obs as (

    select series_id, date, value
    from {{ ref('stg_fred__series') }}
    where value is not null
      and series_id in ({% for id in ids %}'{{ id }}'{{ ", " if not loop.last }}{% endfor %})

), ends as (

    select
        {% for id in ids %}max(date) filter (where series_id = '{{ id }}') as "{{ id }}_end"{{ "," if not loop.last }}
        {% endfor %}
    from obs

), wide as (

    pivot obs
    on series_id in ({% for id in ids %}'{{ id }}'{{ ", " if not loop.last }}{% endfor %})
    using any_value(value)
    group by date

), filled as (

    select
        date,
        {% for id in ids %}last_value("{{ id }}" ignore nulls) over w as "{{ id }}"{{ "," if not loop.last }}
        {% endfor %}
    from wide
    window w as (order by date rows between unbounded preceding and current row)

), levels as (

    -- The last observation on or before each session.
    select s.date, f.* exclude (date)
    from sessions s
    asof left join filled f on s.date >= f.date

), changes as (

    select
        l.date,
        {% for name, d in defs.items() %}
        case when l.date <= e."{{ d[0] }}_end" then
            {% if d[1] == 'log' %}
            case when l."{{ d[0] }}" > 0 and lag(l."{{ d[0] }}") over w > 0
                 then ln(l."{{ d[0] }}" / lag(l."{{ d[0] }}") over w) end
            {% else %}
            l."{{ d[0] }}" - lag(l."{{ d[0] }}") over w
            {% endif %}
        end                                                         as {{ name }},
        {% endfor %}
        list_filter([
            {% for name, d in defs.items() %}case when l.date > e."{{ d[0] }}_end" then '{{ name }}' end,
            {% endfor %}case when l.date > e."DGS10_end" or l.date > e."DGS2_end"
                 then 'd_curve_10y_2y' end
        ], x -> x is not null)                                      as pending
    from levels l
    cross join ends e
    window w as (order by l.date)

)

select
    date,
    {% for name in defs %}{{ name }},
    {% endfor %}d_t_10y - d_t_2y                                        as d_curve_10y_2y,
    pending
from changes
order by date

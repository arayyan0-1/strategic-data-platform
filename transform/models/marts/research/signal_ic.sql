{# Daily cross-sectional Spearman IC between each signal at D and each label (forward return or
   forward residual) over each horizon. One query per label: a single query for all six would not
   fit the build's memory. #}
{%- set horizons = [1, 5, 21] %}
{%- set targets = [('return', 'fwd_ret'), ('residual', 'fwd_resid')] %}

with per_label as (

    {%- for target, column in targets %}{% set t = loop.index0 %}
    {%- for h in horizons %}
    select
        date, sid, {{ t }}::utinyint as t, {{ h }}::utinyint as h,
        corr(r_value, r_label) as ic,
        count(*)               as n_names
    from (

        -- The signal is an integer key because a window sorts integers faster than text.
        -- Average ranks follow the rule in signal_panel.
        select
            date, sid,
            (rank() over wv + count(*) over wv) / 2.0 as r_value,
            (rank() over wl + count(*) over wl) / 2.0 as r_label
        from (
            select date, {{ signal_id('signal') }} as sid, value, {{ column }}_{{ h }} as label
            from {{ ref('signal_panel') }}
            where {{ column }}_{{ h }} is not null
        )
        window
            wv as (partition by date, sid order by value
                   range between unbounded preceding and current row),
            wl as (partition by date, sid order by label
                   range between unbounded preceding and current row)

    )
    group by date, sid
    having count(*) >= {{ var('ic_min_names') }}
    {{ "union all" if not (loop.last and target == targets[-1][0]) }}
    {%- endfor %}
    {%- endfor %}

)

select
    date,
    {{ signal_name('sid') }}                                    as signal,
    [{% for target, _ in targets %}'{{ target }}'{{ ", " if not loop.last }}{% endfor %}][t + 1] as target,
    h::integer                                                  as horizon,
    ic,
    n_names
from per_label

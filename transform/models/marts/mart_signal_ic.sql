{#
  The daily cross-sectional rank correlation (Spearman IC) between each signal
  at D and a label over each horizon, over in_universe names. The target return is
  the forward return. The target residual is the forward residual of the style and
  industry model: the part of the return that the known factors do not explain. The
  value and the label get a new rank over the names that have the label. Tied values
  get the average of the ranks that they occupy.
#}
{%- set horizons = [1, 5, 21] %}
{%- set targets = [('return', 'fwd_ret'), ('residual', 'fwd_resid')] %}

with joined as (

    -- The signal, the target and the horizon are integer keys, because a window
    -- sorts on an integer key faster than on text.
    {%- for target, column in targets %}{% set t = loop.index0 %}
    {%- for h in horizons %}
    select
        date, {{ signal_id('signal') }} as sid, {{ t }}::utinyint as t, {{ h }}::utinyint as h,
        value, {{ column }}_{{ h }} as label
    from {{ ref('mart_signal_panel') }}
    where {{ column }}_{{ h }} is not null
    {{ "union all" if not (loop.last and target == targets[-1][0]) }}
    {%- endfor %}
    {%- endfor %}

), ranked as (

    -- The average rank, by the same rule as mart_signal_panel.
    select
        date, sid, t, h,
        (rank() over wv + count(*) over wv) / 2.0 as r_value,
        (rank() over wl + count(*) over wl) / 2.0 as r_label
    from joined
    window
        wv as (partition by date, sid, t, h order by value
               range between unbounded preceding and current row),
        wl as (partition by date, sid, t, h order by label
               range between unbounded preceding and current row)

)

select
    date,
    {{ signal_name('sid') }}                                    as signal,
    [{% for target, _ in targets %}'{{ target }}'{{ ", " if not loop.last }}{% endfor %}][t + 1] as target,
    h::integer                                                  as horizon,
    corr(r_value, r_label)                                      as ic,
    count(*)                                                    as n
from ranked
group by date, sid, t, h
having count(*) >= {{ var('ic_min_names') }}

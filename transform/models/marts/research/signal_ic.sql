{#
  The daily cross-sectional rank correlation (Spearman IC) between each signal
  at D and a label over each horizon, over in_universe names. The target return is
  the forward return. The target residual is the forward residual of the style and
  industry model: the part of the return that the known factors do not explain. The
  value and the label get a new rank over the names that have the label. Tied values
  get the average of the ranks that they occupy.

  Each label has its own query. One query for all six labels sorts 200 million rows
  at once and needs more memory than the build has. Six sorts of 34 million rows
  take less than half the time.
#}
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

        -- The signal is an integer key, because a window sorts on an integer
        -- key faster than on text. The average rank is by the same rule as
        -- signal_panel.
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

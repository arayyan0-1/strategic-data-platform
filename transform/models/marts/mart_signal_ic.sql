{#
  The daily cross-sectional rank correlation (Spearman IC) between each signal
  at D and the forward return over each horizon, over in_universe names. The
  value and the forward return get a new rank over the names that have a
  forward return. Tied values get the average of the ranks that they occupy.
#}
{%- set horizons = [1, 5, 21] %}

with joined as (

    -- The signal and the horizon are integer keys, because a window sorts on an
    -- integer key faster than on text.
    {%- for h in horizons %}
    select
        date, {{ signal_id('signal') }} as sid, {{ h }}::utinyint as h,
        value, fwd_ret_{{ h }} as fwd_ret
    from {{ ref('mart_signal_panel') }}
    where fwd_ret_{{ h }} is not null
    {{ "union all" if not loop.last }}
    {%- endfor %}

), ranked as (

    -- The average rank, by the same rule as mart_signal_panel.
    select
        date, sid, h,
        (rank() over wv + count(*) over wv) / 2.0 as r_value,
        (rank() over wf + count(*) over wf) / 2.0 as r_fwd
    from joined
    window
        wv as (partition by date, sid, h order by value
               range between unbounded preceding and current row),
        wf as (partition by date, sid, h order by fwd_ret
               range between unbounded preceding and current row)

)

select
    date,
    {{ signal_name('sid') }}  as signal,
    'fwd_ret_' || h           as horizon,
    corr(r_value, r_fwd)      as ic,
    count(*)                  as n
from ranked
group by date, sid, h
having count(*) >= {{ var('ic_min_names') }}

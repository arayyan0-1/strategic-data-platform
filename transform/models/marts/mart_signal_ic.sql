{#
  The daily cross-sectional rank correlation (Spearman IC) between each signal
  at D and the forward return over each horizon, over in_universe names. The
  value and the forward return get a new rank over the names that have a
  forward return. Tied values get the average of the ranks that they occupy.
#}

with joined as (

    select date, signal, horizon, value, fwd_ret
    from (
        unpivot (
            select date, signal, value, fwd_ret_1, fwd_ret_5, fwd_ret_21
            from {{ ref('mart_signal_panel') }}
        )
        on fwd_ret_1, fwd_ret_5, fwd_ret_21
        into name horizon value fwd_ret
    )
    where fwd_ret is not null

), ranked as (

    -- The average rank, by the same rule as mart_signal_panel.
    select
        date, signal, horizon,
        (rank() over wv + count(*) over wv) / 2.0 as r_value,
        (rank() over wf + count(*) over wf) / 2.0 as r_fwd
    from joined
    window
        wv as (partition by date, signal, horizon order by value
               range between unbounded preceding and current row),
        wf as (partition by date, signal, horizon order by fwd_ret
               range between unbounded preceding and current row)

)

select
    date, signal, horizon,
    corr(r_value, r_fwd) as ic,
    count(*)             as n
from ranked
group by date, signal, horizon
having count(*) >= {{ var('ic_min_names') }}

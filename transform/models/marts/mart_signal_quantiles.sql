{#
  Signal shape. Each session, in_universe names are sorted into five groups by
  the signal, and the forward return is averaged in each group over all
  sessions. A monotone rise from group 1 to 5 is the signal at work. The group
  is the panel quintile, so tied values share one group. A signal with many
  tied values can leave a group empty.
#}

select
    signal,
    quintile,
    count(*)          as n,
    avg(fwd_ret_1)    as mean_fwd_1,
    avg(fwd_ret_5)    as mean_fwd_5,
    avg(fwd_ret_21)   as mean_fwd_21
from {{ ref('mart_signal_panel') }}
group by signal, quintile
order by signal, quintile

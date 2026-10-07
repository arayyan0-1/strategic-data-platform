{# Mean forward return by panel quintile of each signal. A signal with many tied values can leave
   a quintile empty. #}

select
    signal,
    quintile,
    count(*)          as n_obs,
    avg(fwd_ret_1)    as mean_fwd_1,
    avg(fwd_ret_5)    as mean_fwd_5,
    avg(fwd_ret_21)   as mean_fwd_21
from {{ ref('signal_panel') }}
group by signal, quintile
order by signal, quintile

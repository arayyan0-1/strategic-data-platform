{#
  The latest session. Each signal value, its z-score, decile and rank, for each
  in_universe name. A description of the present tilt, not a recommendation.
  The decile comes from the panel avg_rank, so tied values share one decile.
#}

with latest as (

    select max(date) as d
    from {{ ref('mart_signal_panel') }}

), sig as (

    select security_key, ticker, date, signal, value, n, avg_rank
    from {{ ref('mart_signal_panel') }}
    where date = (select d from latest)

)

select
    date,
    signal,
    ticker,
    security_key,
    value,
    (value - avg(value) over p)
        / nullif(stddev_samp(value) over p, 0)          as z,
    cast(ceil(avg_rank * 10.0 / n) as integer)           as decile,
    rank() over (partition by signal order by value desc) as rank_high,
    n
from sig
window p as (partition by signal)

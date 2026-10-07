{# The latest session: each in-universe name's signal value, z-score, decile (from the panel
   avg_rank) and rank. #}

with latest as (

    select max(date) as d
    from {{ ref('signal_panel') }}

), sig as (

    select security_key, ticker, date, signal, value, n_names, avg_rank
    from {{ ref('signal_panel') }}
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
    cast(ceil(avg_rank * 10.0 / n_names) as integer)           as decile,
    rank() over (partition by signal order by value desc) as rank_high,
    n_names
from sig
window p as (partition by signal)

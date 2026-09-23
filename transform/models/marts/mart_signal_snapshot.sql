{#
  The latest session. Each signal value, its z-score, decile and rank, for each
  in_universe name. A description of the present tilt, not a recommendation.
#}

with latest as (

    select max(date) as d
    from {{ ref('mart_signals') }}
    where in_universe

), sig as (

    select security_key, ticker, date, signal, value
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe
      and date = (select d from latest)
      and value is not null

)

select
    date,
    signal,
    ticker,
    security_key,
    value,
    (value - avg(value) over p)
        / nullif(stddev_samp(value) over p, 0)          as z,
    ntile(10) over (partition by signal order by value)  as decile,
    rank() over (partition by signal order by value desc) as rank_high,
    count(*) over p                                       as n
from sig
window p as (partition by signal)

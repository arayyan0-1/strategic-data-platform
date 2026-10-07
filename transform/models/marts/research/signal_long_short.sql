{# Top-quintile less bottom-quintile mean forward return per signal: equal weight, rebalanced
   daily, gross of cost. ret is unclipped, so it keeps the squeezes a portfolio earns.
   ret_winsorized (a robustness check only) clips each session's returns. #}

with bins as (

    -- Tied values can leave bin 1 or 5 empty, so the sides are the lowest and highest bins with
    -- names. Same rule as signal_turnover.
    select
        signal, date, quintile, fwd_ret_1,
        min(quintile) over (partition by date, signal) as bottom,
        max(quintile) over (partition by date, signal) as top
    from {{ ref('signal_panel') }}

), binned as (

    select * from bins
    where fwd_ret_1 is not null

), caps as (

    select
        signal, date,
        quantile_cont(fwd_ret_1, {{ var('long_short_winsor') }})     as lo,
        quantile_cont(fwd_ret_1, 1 - {{ var('long_short_winsor') }}) as hi
    from binned
    group by signal, date

), capped as (

    select
        b.signal, b.date, b.quintile, b.bottom, b.top, b.fwd_ret_1,
        least(greatest(b.fwd_ret_1, c.lo), c.hi) as w_ret
    from binned b
    inner join caps c on c.signal = b.signal and c.date = b.date

), daily as (

    -- A session where all names share one bin has no short side, so ret is null.
    select
        signal,
        date,
        avg(fwd_ret_1) filter (where quintile = top)
            - avg(fwd_ret_1) filter (where quintile = bottom and bottom < top) as ret,
        avg(w_ret) filter (where quintile = top)
            - avg(w_ret) filter (where quintile = bottom and bottom < top)     as ret_winsorized
    from capped
    group by signal, date

)

select
    signal,
    date,
    ret,
    sum(ret) over (
        partition by signal order by date
        rows between unbounded preceding and current row) as cum_ret,
    ret_winsorized
from daily

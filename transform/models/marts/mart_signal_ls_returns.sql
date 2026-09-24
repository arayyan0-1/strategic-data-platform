{#
  Long-short return of each signal: the top quintile less the bottom, equal
  weighted, rebalanced daily. The bins are the panel quintiles, so tied values
  share one bin. Many tied values can leave bin 1 or bin 5 empty, so each side
  is the lowest or highest bin that has names. The forward return is winsorized
  to its 1st and 99th percentile each session, so one extreme print does not
  dominate. The IC needs no such step, because a rank is already robust. cum_ls
  is the arithmetic sum, gross of cost. Read it beside mart_signal_turnover.
#}

with bins as (

    -- The top and bottom bins come from all names in the session, as in
    -- mart_signal_turnover.
    select
        signal, date, quintile, fwd_ret_1,
        min(quintile) over (partition by date, signal) as bottom,
        max(quintile) over (partition by date, signal) as top
    from {{ ref('mart_signal_panel') }}

), binned as (

    select * from bins
    where fwd_ret_1 is not null

), caps as (

    select
        signal, date,
        quantile_cont(fwd_ret_1, 0.01) as lo,
        quantile_cont(fwd_ret_1, 0.99) as hi
    from binned
    group by signal, date

), capped as (

    select
        b.signal, b.date, b.quintile, b.bottom, b.top,
        least(greatest(b.fwd_ret_1, c.lo), c.hi) as w_ret
    from binned b
    inner join caps c on c.signal = b.signal and c.date = b.date

), daily as (

    -- A session where all names share one bin has no short side, so ls_ret is null.
    select
        signal,
        date,
        avg(w_ret) filter (where quintile = top)
            - avg(w_ret) filter (where quintile = bottom and bottom < top) as ls_ret
    from capped
    group by signal, date

)

select
    signal,
    date,
    ls_ret,
    sum(ls_ret) over (
        partition by signal order by date
        rows between unbounded preceding and current row) as cum_ls
from daily

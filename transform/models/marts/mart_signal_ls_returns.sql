{#
  Long-short return of each signal: the top quintile less the bottom, equal
  weighted, rebalanced daily. The forward return is winsorized to its 1st and
  99th percentile each session, so one extreme print does not dominate. The IC
  needs no such step, because a rank is already robust. cum_ls is the arithmetic
  sum, gross of cost. Read it beside mart_signal_turnover.
#}

with sig as (

    select security_key, date, signal, value
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe

), fwd as (

    select security_key, date, fwd_ret_1
    from {{ ref('mart_forward_returns') }}

), binned as (

    select
        s.signal, s.date, f.fwd_ret_1,
        ntile(5) over (partition by s.date, s.signal order by s.value) as quintile
    from sig s
    inner join fwd f on f.security_key = s.security_key and f.date = s.date
    where s.value is not null and f.fwd_ret_1 is not null

), caps as (

    select
        signal, date,
        quantile_cont(fwd_ret_1, 0.01) as lo,
        quantile_cont(fwd_ret_1, 0.99) as hi
    from binned
    group by signal, date

), capped as (

    select
        b.signal, b.date, b.quintile,
        least(greatest(b.fwd_ret_1, c.lo), c.hi) as w_ret
    from binned b
    inner join caps c on c.signal = b.signal and c.date = b.date

), daily as (

    select
        signal,
        date,
        avg(w_ret) filter (where quintile = 5)
            - avg(w_ret) filter (where quintile = 1)  as ls_ret
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

-- In each session and signal, the sum of the average ranks of n names is
-- n(n+1)/2, and all names with the same value share one rank. A rank() without
-- the tie term gives a smaller sum. A tie broken in an arbitrary order gives two
-- ranks for one value. The quintiles are in 1 to 5, and each value in a bin is
-- higher than all values in the bins below it. Check the last 20 sessions.
with recent as (

    select distinct date
    from {{ ref('mart_signal_panel') }}
    order by date desc
    limit 20

), p as (

    select p.date, p.signal, p.value, p.n, p.avg_rank, p.quintile
    from {{ ref('mart_signal_panel') }} p
    inner join recent r on r.date = p.date

), bad_sum as (

    select date, signal, 'rank sum is not n(n+1)/2' as failure
    from p
    group by date, signal
    having count(*) <> max(n)
        or sum(avg_rank) <> max(n) * (max(n) + 1) / 2.0

), bad_tie as (

    select distinct date, signal, 'tied values have different ranks' as failure
    from p
    group by date, signal, value
    having count(distinct avg_rank) > 1

), bins as (

    select
        date, signal, quintile,
        min(value) as lo,
        lag(max(value)) over (partition by date, signal order by quintile) as prev_hi
    from p
    group by date, signal, quintile

), bad_bin as (

    select distinct date, signal, 'quintiles are out of range or out of order' as failure
    from bins
    where quintile not between 1 and 5
       or lo <= prev_hi

)

select * from bad_sum
union all
select * from bad_tie
union all
select * from bad_bin

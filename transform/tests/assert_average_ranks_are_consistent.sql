{# A returned row is a session and signal (last 20 sessions) where average ranks do not sum to
   n(n+1)/2, tied values differ in rank, or quintiles are out of range or order. #}
with recent as (

    select distinct date
    from {{ ref('signal_panel') }}
    order by date desc
    limit 20

), p as (

    select p.date, p.signal, p.value, p.n_names, p.avg_rank, p.quintile
    from {{ ref('signal_panel') }} p
    inner join recent r on r.date = p.date

), bad_sum as (

    select date, signal, 'rank sum is not n(n+1)/2' as failure
    from p
    group by date, signal
    having count(*) <> max(n_names)
        or sum(avg_rank) <> max(n_names) * (max(n_names) + 1) / 2.0

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

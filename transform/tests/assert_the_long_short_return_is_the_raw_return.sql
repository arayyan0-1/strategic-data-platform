-- The long-short return of a session is the raw mean forward return of the top bin less
-- the raw mean of the bottom bin. This test recomputes it from the panel with joins on
-- the extreme bins, not with the windows of the model, and compares over the last 60
-- sessions. A session with no return in the model must have none here, and the reverse.
with recent as (

    select distinct date
    from {{ ref('signal_panel') }}
    order by date desc
    limit 60

), extremes as (

    select signal, date, min(quintile) as bottom, max(quintile) as top
    from {{ ref('signal_panel') }}
    where date in (select date from recent)
    group by signal, date

), top_mean as (

    select e.signal, e.date, avg(p.fwd_ret_1) as mean_ret
    from extremes e
    inner join {{ ref('signal_panel') }} p
        on p.signal = e.signal and p.date = e.date and p.quintile = e.top
    group by e.signal, e.date

), bottom_mean as (

    select e.signal, e.date, avg(p.fwd_ret_1) as mean_ret
    from extremes e
    inner join {{ ref('signal_panel') }} p
        on p.signal = e.signal and p.date = e.date and p.quintile = e.bottom
    group by e.signal, e.date

), recomputed as (

    -- With one bin, there is no short side and the return is null.
    select
        e.signal,
        e.date,
        case when e.bottom < e.top then t.mean_ret - b.mean_ret end as ret
    from extremes e
    inner join top_mean t on t.signal = e.signal and t.date = e.date
    inner join bottom_mean b on b.signal = e.signal and b.date = e.date

)

select r.signal, r.date, r.ret as ret_recomputed, l.ret
from recomputed r
left join {{ ref('signal_long_short') }} l
    on l.signal = r.signal and l.date = r.date
where (r.ret is null) <> (l.ret is null)
   or abs(r.ret - l.ret) > 1e-12

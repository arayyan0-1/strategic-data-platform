{#
  The average stock-specific return (the residual of the style and industry model)
  around four kinds of event, from 5 sessions before to 21 after. Day 0 is the event:

  - shock_up and shock_down: a residual of 3 sigma or more (up or down) for a name with a
    cap of $2B or more. Does the move continue or reverse?
  - split_forward and split_reverse: the execution date of a split.
  - short_jump: the date on which FINRA short interest that rose 50% or more since the
    settlement before becomes public, for a name with 2 days to cover or more.

  car is the mean of the residuals summed from day -5, car_post the mean summed from
  day +1, the part that a trade after the event could earn, and t_post its t-statistic.
  Events on the same days move together, so t_post is too high when events cluster.
  Only the development sessions count (before holdout_start), so the holdout stays clean.
#}

with idx as (

    select security_key, date, resid, resid_z, weight_cap,
           row_number() over (partition by security_key order by date) as i
    from {{ ref('style_residuals') }}
    where resid is not null

), lines as (

    select ticker, date, security_key
    from {{ ref('int_universe') }}
    where is_primary_line

), short_jumps as (

    select ticker, effective_date as date
    from (
        select ticker, effective_date, days_to_cover, short_interest,
               lag(short_interest) over (partition by ticker order by settlement_date) as prev
        from {{ ref('int_short_interest') }}
    )
    where prev > 0 and short_interest >= 1.5 * prev and days_to_cover >= 2

), events as (

    select 'shock_up' as event, security_key, i as i0, date
    from idx where resid_z >= 3 and weight_cap >= 2e9
    union all
    select 'shock_down', security_key, i, date
    from idx where resid_z <= -3 and weight_cap >= 2e9
    union all
    select case when c.ratio < 1 then 'split_forward' else 'split_reverse' end,
           x.security_key, x.i, x.date
    from {{ ref('int_corporate_actions') }} c
    join lines l on l.ticker = c.ticker and l.date = c.event_date
    join idx x on x.security_key = l.security_key and x.date = l.date
    where c.kind = 'split' and c.ratio is not null and c.ratio <> 1
    union all
    select 'short_jump', x.security_key, x.i, x.date
    from short_jumps j
    join lines l on l.ticker = j.ticker and l.date = j.date
    join idx x on x.security_key = l.security_key and x.date = l.date

), windows as (

    select e.event, e.security_key, e.i0, x.i - e.i0 as day, x.resid
    from events e
    join idx x on x.security_key = e.security_key and x.i between e.i0 - 5 and e.i0 + 21
    where e.date < date '{{ var("holdout_start") }}'

), cumulated as (

    select
        *,
        sum(resid) over w                                         as car,
        case when day >= 1 then sum(case when day >= 1 then resid else 0 end) over w end
                                                                  as car_post
    from windows
    window w as (partition by event, security_key, i0 order by day)

)

select
    event,
    day,
    count(*)                                                      as n_events,
    avg(resid)                                                    as mean_resid,
    avg(car)                                                      as car,
    avg(car_post)                                                 as car_post,
    avg(car_post) / nullif(stddev_samp(car_post) / sqrt(count(car_post)), 0) as t_post
from cumulated
group by event, day
order by event, day

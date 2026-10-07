{# Mean stock-specific (style-model residual) return from 5 sessions before to 21 after five
   kinds of event (day 0). Means and t are across event dates, since events on one date move
   together. Only development sessions count, so the holdout stays clean. #}

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

), sessions as (

    -- The development sessions, numbered from the last one (1).
    select date, row_number() over (order by date desc) as back
    from (select distinct date from idx where date < date '{{ var("holdout_start") }}')

), windows as (

    select e.event, e.security_key, e.i0, e.date, x.i - e.i0 as day, x.resid
    from events e
    join sessions s on s.date = e.date and s.back > 21
    join idx x on x.security_key = e.security_key and x.i between e.i0 - 5 and e.i0 + 21
        and x.date < date '{{ var("holdout_start") }}'

), cumulated as (

    select
        *,
        sum(resid) over w                                         as car,
        case when day >= 1 then sum(case when day >= 1 then resid else 0 end) over w end
                                                                  as car_post
    from windows
    window w as (partition by event, security_key, i0 order by day)

), by_date as (

    select
        event, day, date,
        count(*)                                                  as n_events,
        avg(resid)                                                as mean_resid,
        avg(car)                                                  as car,
        avg(car_post)                                             as car_post
    from cumulated
    group by event, day, date

)

select
    event,
    day,
    sum(n_events)::bigint                                         as n_events,
    count(*)                                                      as n_dates,
    avg(mean_resid)                                               as mean_resid,
    avg(car)                                                      as car,
    avg(car_post)                                                 as car_post,
    avg(car_post) / nullif(stddev_samp(car_post) / sqrt(count(car_post)), 0) as t_post
from by_date
group by event, day
order by event, day

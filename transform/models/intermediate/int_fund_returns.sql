{# Daily total return of each fund line (primary line, type in fund_types). ret is null without a
   bar on the previous session, so a return never spans two sessions. adv is the trailing dollar
   volume from int_universe. #}
{{ config(materialized='view') }}

with sessions as (

    select date, lag(date) over (order by date) as prev_date
    from (select distinct date from {{ ref('int_universe') }})

), bars as (

    select
        u.security_key,
        p.ticker,
        p.date,
        u.type_filled                                       as type,
        u.adv,
        p.adj_close_total,
        lag(p.date) over w                                  as prev_row_date,
        lag(p.adj_close_total) over w                       as prev_adj_close_total
    from {{ ref('int_prices_adjusted') }} p
    inner join {{ ref('int_universe') }} u
        on u.ticker = p.ticker and u.date = p.date
    where u.is_primary_line
      and u.type_filled in ({{ sql_list('fund_types') }})
    window w as (partition by u.security_key order by p.date)

)

select
    b.security_key,
    b.ticker,
    b.date,
    b.type,
    b.adv,
    case when b.prev_row_date = s.prev_date
         then b.adj_close_total / nullif(b.prev_adj_close_total, 0) - 1
    end                                                     as ret
from bars b
inner join sessions s using (date)

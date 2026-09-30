{#
  The daily total return of each fund line, on each session with a bar. A fund line is the
  primary line of a security with a type in fund_types. These are the funds that the
  universe leaves out. The return is the adjusted close of the session over the adjusted
  close of the session before, less 1. It is null when the line has no bar on the session
  before, so a return never spans two sessions. adv is the trailing dollar volume of
  int_universe.
#}
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

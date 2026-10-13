{# A returned row is a month of bond:ust_10y where the return is more than 0.002 from
   -D * dy + y / 12, with y the 10-year yield at the end of the month before, dy its change in the
   month, and D the modified duration of a 10-year par bond at y: (1 - (1 + y/2)^-20) / y. Only
   months with |dy| below 0.005 are checked; a larger move has a convexity term near the margin.
   The yields are taken again from FRED here. A model month with no pair of yields is returned. #}
with ends as (

    select last_day(date) as month, arg_max(value, date) / 100 as y
    from {{ ref('stg_fred__series') }}
    where series_id = 'DGS10' and value is not null
    group by 1

), pairs as (

    select c.month, p.y as y0, c.y - p.y as dy
    from ends c
    join ends p on p.month = last_day(c.month - interval 1 month)

)

select
    r.month,
    r.ret,
    -(1 - pow(1 + p.y0 / 2, -20)) / p.y0 * p.dy + p.y0 / 12   as approximation
from {{ ref('int_index_returns') }} r
left join pairs p using (month)
where r.index_key = 'bond:ust_10y'
  and (p.month is null
       or (abs(p.dy) < 0.005
           and abs(r.ret - (-(1 - pow(1 + p.y0 / 2, -20)) / p.y0 * p.dy + p.y0 / 12)) > 0.002))

{#
  The total-adjusted close of each monitor ETF over the last two years, for charts.
  The ETF is the security that holds its ticker on the last session.
#}

with last_session as (

    select max(date) as d from {{ ref('stg_prices_adjusted') }}

), keys as (

    select e.ticker, u.security_key
    from {{ ref('monitor_etfs') }} e
    join {{ ref('stg_universe') }} u
        on u.ticker = e.ticker and u.date = (select d from last_session)

)

select
    k.ticker,
    p.date,
    coalesce(p.adj_close_total, p.adj_close_split) as px
from keys k
join {{ ref('stg_universe') }} u
    on u.security_key = k.security_key and u.is_primary_line
join {{ ref('stg_prices_adjusted') }} p
    on p.ticker = u.ticker and p.date = u.date
where p.date > (select d from last_session) - interval 2 year

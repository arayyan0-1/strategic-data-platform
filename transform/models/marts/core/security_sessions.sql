{#
  One row per security and session: the primary line of the security, its unadjusted
  and adjusted prices, and its universe state. A notebook that needs a price series
  reads this view, so a ticker change stays one series and a reused ticker is two.
#}

{{ config(materialized='view') }}

select
    u.security_key,
    p.date,
    p.ticker,
    u.name,
    u.type_filled            as type,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume,
    p.dollar_volume,
    p.adj_open_split,
    p.adj_high_split,
    p.adj_low_split,
    p.adj_close_split,
    p.adj_close_total,
    u.adv,
    u.market_cap,
    u.industry,
    u.industry_name,
    u.in_universe
from {{ ref('int_prices_adjusted') }} p
inner join {{ ref('int_universe') }} u
    on u.ticker = p.ticker and u.date = p.date
where u.is_primary_line

{# One row per security and session from its primary line, with prices and universe state.
   The price series for research code: a ticker change stays one series and a reused ticker is two. #}

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

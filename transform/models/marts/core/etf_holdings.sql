{{ config(materialized='view') }}

{# Holdings per fund, as-of date and line, for research code. #}

select
    fund, date, position, issuer, security_key, ticker, name, sector, asset_class,
    weight, quantity, market_value, price, location, currency, cusip, isin, sedol,
    coupon, maturity, duration, ytm
from {{ ref('int_etf_holdings') }}

{# The ticker reference per date. An empty CIK is null. #}

select
    ticker,
    cast(date as date)      as date,
    name,
    type,
    primary_exchange,
    active,
    currency_name,
    nullif(cik, '')         as cik,
    composite_figi,
    share_class_figi
from {{ source('massive', 'tickers') }}

{# Share counts and cap per month-end. market_cap is for the whole company, not the share class. #}

select
    ticker,
    cast(date as date)                  as date,
    share_class_shares_outstanding,
    market_cap,
    nullif(sic_code, '')                as sic_code,
    sic_description
from {{ source('massive', 'ticker_details') }}

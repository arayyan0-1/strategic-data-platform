{# FINRA short interest per ticker and settlement date. #}

select
    cast(settlement_date as date)       as settlement_date,
    ticker,
    short_interest,
    avg_daily_volume,
    days_to_cover
from {{ source('massive', 'short_interest') }}

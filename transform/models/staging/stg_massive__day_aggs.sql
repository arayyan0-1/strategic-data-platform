{# One bar per ticker and session, unadjusted. #}

select
    ticker,
    cast(date as date) as date,
    open,
    high,
    low,
    close,
    volume,
    transactions
from {{ source('massive', 'day_aggs') }}
where ticker is not null

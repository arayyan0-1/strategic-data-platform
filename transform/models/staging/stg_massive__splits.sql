{# The split table as the vendor states it now. #}

select
    ticker,
    cast(execution_date as date)        as execution_date,
    split_from,
    split_to,
    adjustment_type,
    historical_adjustment_factor,
    vendor_pull_date
from {{ source('massive', 'splits') }}

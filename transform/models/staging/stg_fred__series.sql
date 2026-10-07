{# FRED series in FRED's units, one row per series and date. A null value is no observation. #}

select
    series_id,
    cast(date as date)      as date,
    value,
    vendor_pull_date
from {{ source('fred', 'series') }}

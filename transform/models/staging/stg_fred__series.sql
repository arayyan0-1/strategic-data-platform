{# The FRED series, one row per series and date, in the units of FRED. A null value is
   a day with no observation. #}

select
    series_id,
    cast(date as date)      as date,
    value,
    vendor_pull_date
from {{ source('fred', 'series') }}

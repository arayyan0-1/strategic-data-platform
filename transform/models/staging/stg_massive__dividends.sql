{# The dividend table as the vendor states it now. #}

select
    ticker,
    cast(ex_dividend_date as date)      as ex_dividend_date,
    cash_amount,
    currency,
    distribution_type,
    frequency,
    historical_adjustment_factor,
    vendor_pull_date
from {{ source('massive', 'dividends') }}

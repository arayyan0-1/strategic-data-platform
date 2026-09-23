-- The vendor gives 999.99 for days_to_cover when the true ratio is 999.99 or more.
-- This cap is not a measurement, so the staging model must set it to null.
select
    settlement_date,
    ticker,
    days_to_cover
from {{ ref('stg_short_interest') }}
where days_to_cover >= 999.99

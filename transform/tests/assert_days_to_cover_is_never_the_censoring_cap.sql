{# A returned row is a days_to_cover at the vendor's 999.99 cap, which must be null. #}
select
    settlement_date,
    ticker,
    days_to_cover
from {{ ref('int_short_interest') }}
where days_to_cover >= 999.99

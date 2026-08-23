-- The vendor caps days_to_cover at 999.99. The cap is not a measurement.
--
-- Decision 0009 first recorded the cap as a sentinel for avg_daily_volume = 0.
-- That is incomplete. Measured on four settlement dates, 15,317 rows hold the
-- cap and only 12,074 of them have zero volume. The rest have an implied ratio
-- above the cap, up to 21.4 million, while the largest value below the cap is
-- 998.82.
--
-- So a rule that nulls the field only where volume is zero leaves thousands of
-- capped values in place, on the least liquid names, which is the exact set that
-- a crowding signal must treat with care.
select
    settlement_date,
    ticker,
    days_to_cover
from {{ ref('stg_short_interest') }}
where days_to_cover >= 999.99

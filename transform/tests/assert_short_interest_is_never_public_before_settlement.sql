{# A returned row is a short-interest number public on or before its settlement date, which would
   let a signal read the future. #}
select
    settlement_date,
    effective_date,
    ticker
from {{ ref('int_short_interest') }}
where effective_date is not null
  and effective_date <= settlement_date

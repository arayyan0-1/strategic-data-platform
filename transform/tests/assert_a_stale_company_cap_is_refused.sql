{# A returned row is a month-end whose cap_source differs from the expected rule (WOLF, HON,
   LLYVA), or a WOLF universe cap that should be null. #}
with cases (ticker, month_end, expected) as (
    values
        ('WOLF',  date '2025-09-30', 'refused'),
        ('WOLF',  date '2025-10-31', 'company_cap'),
        ('HON',   date '2026-06-30', 'company_cap'),
        ('LLYVA', date '2026-08-31', 'class_carried')
)
select 'details' as check_name, c.ticker, c.month_end as date, d.cap_source, d.cap
from cases c
left join {{ ref('int_security_details') }} d
    on d.ticker = c.ticker and d.month_end = c.month_end
where d.cap_source is distinct from c.expected
   or (d.cap is null) <> (c.expected = 'refused')
union all
select 'universe', ticker, date, null, market_cap
from {{ ref('int_universe') }}
where ticker = 'WOLF'
  and date between date '2025-09-30' and date '2025-10-30'
  and market_cap is not null

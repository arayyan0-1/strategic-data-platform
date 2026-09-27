-- WOLF left bankruptcy on 2025-09-29 with 26M new shares. The pull of 2025-09-30 has no
-- class count and values the 156M old shares at the new price, so its cap is refused and
-- the universe has no cap until the next month-end. The pull of 2025-10-31 counts the new
-- shares, and its vendor cap stays. The vendor cap of HON on 2026-06-30 disagrees with the
-- class count before it and agrees with the count after it, so it stays. The pull of LLYVA
-- on 2026-08-31 has no class count and a vendor cap of all three classes, so it takes the
-- class count of the pull before.
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

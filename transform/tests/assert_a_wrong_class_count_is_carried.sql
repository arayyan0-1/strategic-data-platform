{# A returned row is a month-end whose cap_source or cap differs from the expected one for a
   known wrong class count (NFLX, HALO, FAST, COF, KLAC, CRWD, CAST), or a low NFLX universe cap. #}
with cases (ticker, month_end, expected, cap_low, cap_high) as (
    values
        ('NFLX', date '2025-10-31', 'class_shares',  4.0e11, 5.5e11),
        ('NFLX', date '2025-11-28', 'class_carried', 4.0e11, 5.5e11),
        ('NFLX', date '2025-12-31', 'class_shares',  3.5e11, 4.5e11),
        ('HALO', date '2025-07-31', 'class_shares',  6.5e9,  8.5e9),
        ('HALO', date '2025-08-29', 'class_carried', 7.5e9,  1.0e10),
        ('HALO', date '2025-09-30', 'class_shares',  7.5e9,  1.0e10),
        ('FAST', date '2025-06-30', 'class_carried', 4.0e10, 5.5e10),
        ('COF',  date '2025-05-30', 'class_shares',  1.0e11, 1.4e11),
        ('COF',  date '2025-06-30', 'class_carried', 1.2e11, 1.6e11),
        ('KLAC', date '2026-07-31', 'class_carried', 2.0e11, 2.8e11),
        ('CRWD', date '2026-08-31', 'class_shares',  2.0e11, 2.8e11),
        ('CAST', date '2026-08-31', 'class_shares',  3.0e7,  6.0e7)
)
select 'details' as check_name, c.ticker, c.month_end as date, d.cap_source, d.cap
from cases c
left join {{ ref('int_security_details') }} d
    on d.ticker = c.ticker and d.month_end = c.month_end
where d.cap_source is distinct from c.expected
   or d.cap is null
   or d.cap not between c.cap_low and c.cap_high
union all
select 'universe', ticker, date, null, market_cap
from {{ ref('int_universe') }}
where ticker = 'NFLX'
  and date between date '2025-11-28' and date '2025-12-30'
  and (market_cap is null or market_cap < 3.0e11)

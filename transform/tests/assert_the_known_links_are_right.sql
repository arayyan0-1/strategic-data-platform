-- OKE took a new FIGI in a reorganization on 2026-09-11 and stays one security. MSGE is
-- a spin-off: its old FIGI went on as SPHR. HR is a merger: its new FIGI traded as HTA.
-- WOLF left bankruptcy with a sixth of its shares, and its price jumped 18 times. Each
-- of the last three stays two securities.
with cases (ticker, before_date, after_date, one_security) as (
    values
        ('OKE',  date '2026-09-10', date '2026-09-11', true),
        ('MSGE', date '2023-04-20', date '2023-04-21', false),
        ('HR',   date '2022-07-20', date '2022-07-21', false),
        ('WOLF', date '2025-09-26', date '2025-09-29', false)
)
select c.*, b.security_key as key_before, a.security_key as key_after
from cases c
left join {{ ref('int_tickers_keyed') }} b on b.ticker = c.ticker and b.date = c.before_date
left join {{ ref('int_tickers_keyed') }} a on a.ticker = c.ticker and a.date = c.after_date
where b.security_key is null
   or a.security_key is null
   or (a.security_key = b.security_key) <> c.one_security

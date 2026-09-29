-- OKE took a new FIGI in a reorganization on 2026-09-11 and stays one security. XOM did
-- the same on 2026-07-02, when its name changed from Exxon Mobil to ExxonMobil. MSGE is
-- a spin-off: its old FIGI went on as SPHR. HR is a merger: its new FIGI traded as HTA.
-- WOLF left bankruptcy with a sixth of its shares, and its price jumped 18 times. GORO
-- became Goldgroup Mining, with an older FIGI and, three sessions later, the Goldgroup
-- CIK. Each of those four stays two securities. LH moved into a holding company under a
-- new name, and a reviewed decision links it. BYND had another FIGI for one session at
-- a reverse split, and the flicker does not start a security.
with cases (ticker, before_date, after_date, one_security) as (
    values
        ('OKE',  date '2026-09-10', date '2026-09-11', true),
        ('XOM',  date '2026-07-01', date '2026-07-02', true),
        ('MSGE', date '2023-04-20', date '2023-04-21', false),
        ('HR',   date '2022-07-20', date '2022-07-21', false),
        ('WOLF', date '2025-09-26', date '2025-09-29', false),
        ('GORO', date '2026-07-17', date '2026-07-20', false),
        ('LH',   date '2024-05-17', date '2024-05-20', true),
        ('BYND', date '2026-08-12', date '2026-08-13', true)
)
select c.*, b.security_key as key_before, a.security_key as key_after
from cases c
left join {{ ref('int_tickers_keyed') }} b on b.ticker = c.ticker and b.date = c.before_date
left join {{ ref('int_tickers_keyed') }} a on a.ticker = c.ticker and a.date = c.after_date
where b.security_key is null
   or a.security_key is null
   or (a.security_key = b.security_key) <> c.one_security
